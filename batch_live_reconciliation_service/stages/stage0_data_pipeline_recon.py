# SCHEMA_PROVENANCE_EXEMPT: _BlobStats and _ServiceReconResult are private stage-internal
# result containers — not cross-repo contracts.
"""
Stage 0.5 — Data Pipeline Reconciliation.

Compares batch vs live partitions for the data pipeline services:
  - instruments-service  (instruments-store-{category}-{project_id})
  - market-tick-data-service  (market-data-tick-{category}-{project_id})
  - market-data-processing-service  (market-data-tick-{category}-{project_id}/processed/)

For each service+category: reads GCS blobs from batch path and live/ partition,
compares file counts and row counts (for parquet files), and produces a
reconciliation report.

Batch path example:  {date}/venue/instrument_type/
Live path example:   live/{date}/venue/instrument_type/
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO

from unified_trading_library import get_storage_client, log_event, reconcile_shard

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.deviation_thresholds import (
    DATA_PIPELINE_THRESHOLDS,
)
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


@dataclass
class _ServiceReconResult:
    """Reconciliation result for a single data pipeline service + category."""

    service_name: str
    category: str
    bucket: str
    batch_files: int = 0
    live_files: int = 0
    batch_rows: int = 0
    live_rows: int = 0
    has_live_partition: bool = False
    error: str | None = None
    shard_verdict: str | None = None

    @property
    def files_match(self) -> bool:
        """True if file counts are equal (or both zero)."""
        return self.batch_files == self.live_files

    @property
    def rows_match(self) -> bool:
        """True if row counts are equal (or both zero)."""
        return self.batch_rows == self.live_rows


# Data pipeline services to reconcile, with their bucket config attribute prefixes
# and GCS path prefix patterns.
# Each entry: (service_name, bucket_attr_prefix, batch_prefix_template, live_prefix_template)
_DATA_PIPELINE_SERVICES: list[tuple[str, str, str, str]] = [
    (
        "instruments-service",
        "instruments_bucket",
        "{date}/",
        "live/{date}/",
    ),
    (
        "market-tick-data-service",
        "market_data_tick_bucket",
        "{date}/",
        "live/{date}/",
    ),
    (
        "market-data-processing-service",
        "market_data_tick_bucket",
        "processed/{date}/",
        "processed/live/{date}/",
    ),
]

# Asset-group keys (lowercase wire-format SSOT per workspace CLAUDE.md;
# GCS path segments use these literally as ``category=cefi/...``).
# Extended 2026-05-01 (Wave E) to cover sports + prediction. If those
# pipelines have not yet written batch/live archives the reconciliation
# is harmless: the prefix probe returns 0 files and surfaces as a missing
# bucket instead of being silently skipped.
_CATEGORIES = ("cefi", "tradfi", "defi", "sports", "prediction")


@dataclass
class _BlobStats:
    """File count and row count for a GCS prefix."""

    file_count: int = 0
    row_count: int = 0
    errors: list[str] = field(default_factory=list)
    sample_rows: list[dict[str, object]] = field(default_factory=list)


def _count_blobs_and_rows(bucket_name: str, prefix: str) -> _BlobStats:
    """
    Count files and estimate row counts under a GCS prefix.

    For .parquet files, reads the file footer to get row count.
    For .ndjson/.jsonl files, counts newlines.
    For other files, counts 1 row per file as a fallback.
    """
    client = get_storage_client()
    stats = _BlobStats()

    try:
        bucket_obj = client.bucket(bucket_name)
        blobs = list(bucket_obj.list_blobs(prefix=prefix))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as exc:
        msg = f"Error listing gs://{bucket_name}/{prefix}: {exc}"  # noqa: gs-uri — error message, not a URI constructor
        logger.warning("%s", msg)
        stats.errors.append(msg)
        return stats

    for blob in blobs:
        blob_name = str(blob.name)
        # Skip directory markers and _SUCCESS files
        if blob_name.endswith("/") or blob_name.endswith("_SUCCESS"):
            continue

        stats.file_count += 1

        # Try to get row count from parquet metadata
        if blob_name.endswith(".parquet"):
            try:
                raw = client.download_bytes(bucket=bucket_name, blob_path=blob_name)
                stats.row_count += _count_parquet_rows(raw)
                if not stats.sample_rows:
                    stats.sample_rows = _parse_parquet_sample(raw)
            except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as exc:
                msg = f"Error reading parquet gs://{bucket_name}/{blob_name}: {exc}"  # noqa: gs-uri — error message, not a URI constructor
                logger.warning("%s", msg)
                stats.errors.append(msg)
                stats.row_count += 1  # count file itself as minimum
        elif blob_name.endswith(".ndjson") or blob_name.endswith(".jsonl"):
            try:
                raw = client.download_bytes(bucket=bucket_name, blob_path=blob_name)
                line_count = sum(1 for line in raw.decode("utf-8").splitlines() if line.strip())
                stats.row_count += line_count
            except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as exc:
                msg = f"Error reading ndjson gs://{bucket_name}/{blob_name}: {exc}"  # noqa: gs-uri — error message, not a URI constructor
                logger.warning("%s", msg)
                stats.errors.append(msg)
                stats.row_count += 1
        else:
            # CSV or unknown — count as 1 row per file as approximation
            stats.row_count += 1

    return stats


def _count_parquet_rows(raw_bytes: bytes) -> int:
    """Extract row count from parquet file bytes using pyarrow."""
    import pyarrow.parquet as pq  # noqa: qg-inside-import — heavy dep, lazy import

    pf = pq.ParquetFile(BytesIO(raw_bytes))
    return pf.metadata.num_rows


def _parse_parquet_sample(raw_bytes: bytes, max_rows: int = 100) -> list[dict[str, object]]:
    """Parse up to max_rows from parquet bytes as row dicts for reconcile_shard."""
    import pyarrow.parquet as pq  # noqa: qg-inside-import — heavy dep, lazy import

    table = pq.ParquetFile(BytesIO(raw_bytes)).read()
    if table.num_rows == 0:
        return []
    limit = min(table.num_rows, max_rows)
    return [{col: table.column(col)[i].as_py() for col in table.column_names} for i in range(limit)]


def _get_bucket_for_service(config: ReconConfig, bucket_attr_prefix: str, category: str) -> str:
    """Get the bucket name from config for a given service prefix and category."""
    attr_name = f"{bucket_attr_prefix}_{category}"
    bucket_val: str = getattr(config, attr_name, "")
    return bucket_val


def _reconcile_service(
    config: ReconConfig,
    service_name: str,
    bucket_attr_prefix: str,
    batch_prefix_template: str,
    live_prefix_template: str,
    category: str,
    date: str,
) -> _ServiceReconResult:
    """
    Reconcile batch vs live for a single service + category.

    Uses shard-level failure isolation — never raises.
    """
    bucket = _get_bucket_for_service(config, bucket_attr_prefix, category)
    result = _ServiceReconResult(
        service_name=service_name,
        category=category,
        bucket=bucket,
    )

    if not bucket:
        result.error = f"No bucket configured for {service_name}/{category}"
        logger.warning("%s", result.error)
        return result

    batch_prefix = batch_prefix_template.format(date=date)
    live_prefix = live_prefix_template.format(date=date)

    # Count batch blobs
    batch_stats = _count_blobs_and_rows(bucket, batch_prefix)
    result.batch_files = batch_stats.file_count
    result.batch_rows = batch_stats.row_count

    # Count live blobs
    live_stats = _count_blobs_and_rows(bucket, live_prefix)
    result.live_files = live_stats.file_count
    result.live_rows = live_stats.row_count
    result.has_live_partition = live_stats.file_count > 0

    # Deep shard comparison via UTL reconcile_shard() when both sides have parquet data
    if batch_stats.sample_rows and live_stats.sample_rows:
        try:
            shard_report = reconcile_shard(
                asset_group=category,
                venue=service_name,
                data_type="raw",
                instrument_id="sample",
                day=date,
                batch_rows=batch_stats.sample_rows,
                live_rows=live_stats.sample_rows,
            )
            result.shard_verdict = shard_report.verdict
            if shard_report.verdict in ("SCHEMA_MISMATCH", "VALUE_MISMATCH"):
                note = "; ".join(shard_report.notes) if shard_report.notes else ""
                logger.warning(
                    "[Data Pipeline Recon] shard %s/%s verdict=%s notes=%s",
                    service_name,
                    category,
                    shard_report.verdict,
                    note,
                )
        except (ValueError, TypeError, AttributeError, RuntimeError) as exc:
            logger.warning(
                "[Data Pipeline Recon] reconcile_shard failed for %s/%s: %s",
                service_name,
                category,
                exc,
            )

    all_errors = batch_stats.errors + live_stats.errors
    if all_errors:
        result.error = "; ".join(all_errors)

    return result


def _check_deviations(results: list[_ServiceReconResult]) -> list[DeviationRecord]:
    """Return DeviationRecords for data pipeline metrics that breach thresholds."""
    deviations: list[DeviationRecord] = []
    t = DATA_PIPELINE_THRESHOLDS

    # Check per-service file count match rate
    services_with_batch = [r for r in results if r.batch_files > 0]
    if services_with_batch:
        file_matches = sum(1 for r in services_with_batch if r.files_match)
        file_match_rate = file_matches / len(services_with_batch)
        if file_match_rate < t.file_count_match_rate_min:
            mismatched = [
                f"{r.service_name}/{r.category} (batch={r.batch_files}, live={r.live_files})"
                for r in services_with_batch
                if not r.files_match
            ]
            deviations.append(
                DeviationRecord(
                    metric_name="file_count_match_rate",
                    stage=ReconStage.DATA_PIPELINE_RECON,
                    actual_value=file_match_rate,
                    threshold=t.file_count_match_rate_min,
                    direction="below",
                    description=(
                        f"File count match rate {file_match_rate:.1%} "
                        f"< {t.file_count_match_rate_min:.0%} threshold. "
                        f"Mismatched: {', '.join(mismatched)}"
                    ),
                )
            )

    # Check per-service row count match rate
    services_with_rows = [r for r in results if r.batch_rows > 0]
    if services_with_rows:
        row_matches = sum(1 for r in services_with_rows if r.rows_match)
        row_match_rate = row_matches / len(services_with_rows)
        if row_match_rate < t.row_count_match_rate_min:
            mismatched = [
                f"{r.service_name}/{r.category} (batch={r.batch_rows}, live={r.live_rows})"
                for r in services_with_rows
                if not r.rows_match
            ]
            deviations.append(
                DeviationRecord(
                    metric_name="row_count_match_rate",
                    stage=ReconStage.DATA_PIPELINE_RECON,
                    actual_value=row_match_rate,
                    threshold=t.row_count_match_rate_min,
                    direction="below",
                    description=(
                        f"Row count match rate {row_match_rate:.1%} "
                        f"< {t.row_count_match_rate_min:.0%} threshold. "
                        f"Mismatched: {', '.join(mismatched)}"
                    ),
                )
            )

    # Check for services with no live partition at all
    missing_live = [r for r in results if r.batch_files > 0 and not r.has_live_partition]
    if len(missing_live) > t.missing_live_partition_max:
        missing_names = [f"{r.service_name}/{r.category}" for r in missing_live]
        deviations.append(
            DeviationRecord(
                metric_name="missing_live_partitions",
                stage=ReconStage.DATA_PIPELINE_RECON,
                actual_value=float(len(missing_live)),
                threshold=float(t.missing_live_partition_max),
                direction="above",
                description=(
                    f"{len(missing_live)} service(s) have batch data but no live partition: {', '.join(missing_names)}"
                ),
            )
        )

    # Check for schema or value mismatches detected by reconcile_shard()
    schema_mismatches = [r for r in results if r.shard_verdict == "SCHEMA_MISMATCH"]
    if schema_mismatches:
        names = [f"{r.service_name}/{r.category}" for r in schema_mismatches]
        deviations.append(
            DeviationRecord(
                metric_name="shard_schema_mismatch",
                stage=ReconStage.DATA_PIPELINE_RECON,
                actual_value=float(len(schema_mismatches)),
                threshold=0.0,
                direction="above",
                description=(f"{len(schema_mismatches)} service(s) have batch/live schema drift: {', '.join(names)}"),
            )
        )

    value_mismatches = [r for r in results if r.shard_verdict == "VALUE_MISMATCH"]
    if value_mismatches:
        names = [f"{r.service_name}/{r.category}" for r in value_mismatches]
        deviations.append(
            DeviationRecord(
                metric_name="shard_value_mismatch",
                stage=ReconStage.DATA_PIPELINE_RECON,
                actual_value=float(len(value_mismatches)),
                threshold=0.0,
                direction="above",
                description=(f"{len(value_mismatches)} service(s) have batch/live value drift: {', '.join(names)}"),
            )
        )

    return deviations


def run_data_pipeline_recon(config: ReconConfig, date: str, dry_run: bool = False) -> StageReport:
    """
    Run data pipeline reconciliation: compare batch vs live for
    instruments-service, market-tick-data-service, and market-data-processing-service.

    Args:
        config: ReconConfig instance
        date: YYYY-MM-DD date string
        dry_run: If True, skip GCS reads

    Returns:
        StageReport for data pipeline reconciliation
    """
    started_at = datetime.now(UTC)
    log_event(
        "PROCESSING_STARTED",
        details={"stage": "data_pipeline_recon", "date": date},
    )
    logger.info("[Data Pipeline Recon] Reconciliation for date=%s", date)

    if dry_run:
        logger.info("[Data Pipeline Recon] DRY RUN — skipping GCS comparison")
        return StageReport(
            stage=ReconStage.DATA_PIPELINE_RECON,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    results: list[_ServiceReconResult] = []

    for service_name, bucket_attr_prefix, batch_tmpl, live_tmpl in _DATA_PIPELINE_SERVICES:
        for category in _CATEGORIES:
            # Shard-level failure isolation — never raise inside the loop
            try:
                result = _reconcile_service(
                    config=config,
                    service_name=service_name,
                    bucket_attr_prefix=bucket_attr_prefix,
                    batch_prefix_template=batch_tmpl,
                    live_prefix_template=live_tmpl,
                    category=category,
                    date=date,
                )
                results.append(result)
                logger.info(
                    "[Data Pipeline Recon] %s/%s: batch_files=%d live_files=%d "
                    "batch_rows=%d live_rows=%d live_partition=%s",
                    service_name,
                    category,
                    result.batch_files,
                    result.live_files,
                    result.batch_rows,
                    result.live_rows,
                    result.has_live_partition,
                )
            except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as exc:
                msg = f"Error reconciling {service_name}/{category}: {exc}"
                logger.warning("%s", msg)
                results.append(
                    _ServiceReconResult(
                        service_name=service_name,
                        category=category,
                        bucket=_get_bucket_for_service(config, bucket_attr_prefix, category),
                        error=msg,
                    )
                )

    # Compute aggregate metrics
    total_batch_files = sum(r.batch_files for r in results)
    total_live_files = sum(r.live_files for r in results)
    total_batch_rows = sum(r.batch_rows for r in results)
    total_live_rows = sum(r.live_rows for r in results)
    services_with_no_live = sum(1 for r in results if r.batch_files > 0 and not r.has_live_partition)
    services_with_errors = sum(1 for r in results if r.error is not None)

    services_with_schema_mismatch = sum(1 for r in results if r.shard_verdict == "SCHEMA_MISMATCH")
    services_with_value_mismatch = sum(1 for r in results if r.shard_verdict == "VALUE_MISMATCH")

    metrics: dict[str, float] = {
        "total_batch_files": float(total_batch_files),
        "total_live_files": float(total_live_files),
        "total_batch_rows": float(total_batch_rows),
        "total_live_rows": float(total_live_rows),
        "services_checked": float(len(results)),
        "services_with_no_live_partition": float(services_with_no_live),
        "services_with_errors": float(services_with_errors),
        "services_with_schema_mismatch": float(services_with_schema_mismatch),
        "services_with_value_mismatch": float(services_with_value_mismatch),
    }

    deviations = _check_deviations(results)
    status = ReconStatus.PASSED if not deviations else ReconStatus.FAILED

    if deviations:
        logger.warning("[Data Pipeline Recon] %d deviation(s) detected", len(deviations))
        for d in deviations:
            logger.warning("  - %s: %s", d.metric_name, d.description)
    else:
        logger.info("[Data Pipeline Recon] PASSED — no deviations above threshold")

    log_event(
        "PROCESSING_COMPLETED",
        details={
            "stage": "data_pipeline_recon",
            "status": status.value,
            "deviations": len(deviations),
        },
    )

    return StageReport(
        stage=ReconStage.DATA_PIPELINE_RECON,
        status=status,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        deviations=deviations,
        metrics=metrics,
    )
