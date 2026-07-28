"""
Stage 5 — Consolidated Results Writer.

Writes:
  - t1-recon/recon/summary_{date}.json  (all stage summaries)
  - t1-recon/recon/index.json           (append date entry, for API listing)

Emits PubSub success event → alerting-service.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import cast

from unified_trading_library import get_storage_client, log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconReport,
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


def _serialize_deviation(d: DeviationRecord) -> dict[str, object]:
    """Serialize a single deviation for the Stage-5 summary JSON.

    Carries the per-deviation detail (not just the stage's aggregate metrics)
    so downstream consumers — the resolution API's break listing — can
    reconstruct individual breaks from GCS instead of a hardcoded mock.
    """
    return {
        "metric_name": d.metric_name,
        "actual_value": d.actual_value,
        "threshold": d.threshold,
        "direction": d.direction,
        "description": d.description,
        "instrument_id": d.instrument_id,
        "dimension": d.dimension.value,
        "first_seen_at": d.first_seen_at.isoformat(),
    }


def _load_index(bucket: str) -> list[dict[str, object]]:
    """Load existing recon index from GCS, or return empty list."""
    client = get_storage_client()
    try:
        raw = client.download_bytes(bucket=bucket, blob_path="t1-recon/recon/index.json")
        return cast(list[dict[str, object]], json.loads(raw.decode("utf-8")))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError):
        return []


def run_stage5(
    config: ReconConfig,
    report: ReconReport,
    dry_run: bool = False,
) -> StageReport:
    """
    Run Stage 5: Consolidated Results Writer.

    Args:
        config: ReconConfig instance
        report: Full ReconReport (all stages populated)
        dry_run: If True, skip GCS writes

    Returns:
        StageReport for Stage 5
    """
    started_at = datetime.now(UTC)
    date = report.date
    log_event("PROCESSING_STARTED", details={"stage": "stage5_results_writer", "date": date})
    logger.info("[Stage 5] Writing consolidated results for date=%s", date)

    try:
        summary: dict[str, object] = {
            "date": date,
            "run_id": report.run_id,
            "status": report.status.value,
            "total_deviations": report.total_deviations,
            "failed_stages": [s.value for s in report.failed_stages],
            "completed_at": datetime.now(UTC).isoformat(),
            "stages": [
                {
                    "stage": s.stage.value,
                    "status": s.status.value,
                    "deviation_count": s.deviation_count,
                    "metrics": s.metrics,
                    "output_gcs_path": s.output_gcs_path,
                    "error_message": s.error_message,
                    "deviations": [_serialize_deviation(d) for d in s.deviations],
                }
                for s in report.stages
            ],
            "agent_report_gcs_path": report.agent_report_gcs_path,
        }

        summary_blob = f"t1-recon/recon/summary_{date}.json"
        summary_uri = f"gs://{config.recon_bucket}/{summary_blob}"  # noqa: gs-uri

        if not dry_run:
            client = get_storage_client()

            # Write summary
            _ = client.upload_bytes(
                bucket=config.recon_bucket,
                blob_path=summary_blob,
                data=json.dumps(summary, indent=2, default=str).encode("utf-8"),
            )
            logger.info("[Stage 5] Summary written to %s", summary_uri)

            # Update index
            index = _load_index(config.recon_bucket)
            index_entry: dict[str, object] = {
                "date": date,
                "status": report.status.value,
                "total_deviations": report.total_deviations,
                "summary_gcs_path": summary_uri,
                "completed_at": datetime.now(UTC).isoformat(),
            }
            # Remove existing entry for this date if present
            index = [e for e in index if e.get("date") != date]
            index.append(index_entry)
            index.sort(key=lambda e: str(e.get("date") or ""), reverse=True)

            _ = client.upload_bytes(
                bucket=config.recon_bucket,
                blob_path="t1-recon/recon/index.json",
                data=json.dumps(index, indent=2, default=str).encode("utf-8"),
            )
            logger.info("[Stage 5] Index updated")
        else:
            logger.info("[Stage 5] DRY RUN — would write to %s", summary_uri)

        log_event(
            "STOPPED",
            details={
                "stage": "stage5_results_writer",
                "date": date,
                "status": report.status.value,
                "total_deviations": report.total_deviations,
                "summary_uri": summary_uri,
            },
        )

        return StageReport(
            stage=ReconStage.RESULTS_WRITER,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            output_gcs_path=summary_uri,
            metrics={"total_deviations": float(report.total_deviations)},
        )

    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError) as e:
        logger.exception("[Stage 5] Results writer failed: %s", e)
        log_event("FAILED", details={"stage": "stage5_results_writer", "error": str(e)})
        return StageReport(
            stage=ReconStage.RESULTS_WRITER,
            status=ReconStatus.FAILED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            error_message=str(e),
        )
