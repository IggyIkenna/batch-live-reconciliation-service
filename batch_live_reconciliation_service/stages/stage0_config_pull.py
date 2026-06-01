"""
Stage 0 — Config + Data Availability Check.

Polls GCS for:
  - execution config snapshot (configs/snapshots/{date}/)
  - T+1 ML outputs (t1-recon/ml/{date}/)
  - T+1 strategy outputs (t1-recon/strategy/{date}/)

Fails with alert if upstream data is missing after timeout.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import cast

from unified_trading_library import get_storage_client, log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.recon_report import (
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)

# Expected output files per service (poll these to verify upstream jobs completed)
_EXPECTED_OUTPUTS: dict[str, str] = {
    "execution-config-snapshot": "configs/snapshots/{date}/config.json",
    "ml-inference": "t1-recon/ml/{date}/_SUCCESS",
    "strategy": "t1-recon/strategy/{date}/_SUCCESS",
}


def _blob_exists(bucket: str, blob_path: str) -> bool:
    """Check if a GCS blob exists using UCI storage client."""
    client = get_storage_client()
    try:
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.blob(blob_path)
        return bool(blob.exists())
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
        logger.warning("Error checking GCS path gs://%s/%s: %s", bucket, blob_path, e)
        return False


def _load_config_snapshot(execution_store_bucket: str, date: str) -> dict[str, object]:
    """Load frozen execution config from GCS snapshot."""
    blob_path = f"configs/snapshots/{date}/config.json"
    client = get_storage_client()
    try:
        raw_bytes = client.download_bytes(bucket=execution_store_bucket, blob_path=blob_path)
        snapshot: dict[str, object] = cast(dict[str, object], json.loads(raw_bytes.decode("utf-8")))
        logger.info("Loaded config snapshot: %d keys", len(snapshot))
        return snapshot
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
        raise FileNotFoundError(f"Config snapshot not found: gs://{execution_store_bucket}/{blob_path}") from e  # noqa: gs-uri — error message, not a URI constructor


def run_stage0(config: ReconConfig, date: str, dry_run: bool = False) -> StageReport:
    """
    Run Stage 0: Config + Data Availability Check.

    Args:
        config: ReconConfig instance
        date: YYYY-MM-DD date string
        dry_run: If True, skip GCS reads and return synthetic success

    Returns:
        StageReport for Stage 0
    """
    started_at = datetime.now(UTC)
    log_event("PROCESSING_STARTED", details={"stage": "stage0_config_pull", "date": date})
    logger.info("[Stage 0] Config + data availability check for date=%s", date)

    if dry_run:
        logger.info("[Stage 0] DRY RUN — skipping GCS checks")
        return StageReport(
            stage=ReconStage.CONFIG_PULL,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    missing: list[str] = []

    # Check execution config snapshot
    config_blob = _EXPECTED_OUTPUTS["execution-config-snapshot"].format(date=date)
    if not _blob_exists(config.execution_store_bucket, config_blob):
        missing.append(f"execution config snapshot: gs://{config.execution_store_bucket}/{config_blob}")  # noqa: gs-uri — error message, not a URI constructor

    # Check ML outputs
    ml_blob = _EXPECTED_OUTPUTS["ml-inference"].format(date=date)
    if not _blob_exists(config.recon_bucket, ml_blob):
        missing.append(f"ML t1-recon outputs: gs://{config.recon_bucket}/{ml_blob}")  # noqa: gs-uri — error message, not a URI constructor

    # Check strategy outputs
    strategy_blob = _EXPECTED_OUTPUTS["strategy"].format(date=date)
    if not _blob_exists(config.recon_bucket, strategy_blob):
        missing.append(f"strategy t1-recon outputs: gs://{config.recon_bucket}/{strategy_blob}")  # noqa: gs-uri — error message, not a URI constructor

    if missing:
        error_msg = f"Missing upstream data for {date}: {'; '.join(missing)}"
        logger.error("[Stage 0] FAILED — %s", error_msg)
        log_event("PROCESSING_COMPLETED", details={"stage": "stage0_config_pull", "status": "FAILED"})
        return StageReport(
            stage=ReconStage.CONFIG_PULL,
            status=ReconStatus.FAILED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            error_message=error_msg,
        )

    # Load config snapshot for downstream stages
    try:
        _ = _load_config_snapshot(config.execution_store_bucket, date)
    except FileNotFoundError as e:
        log_event("PROCESSING_COMPLETED", details={"stage": "stage0_config_pull", "status": "FAILED"})
        return StageReport(
            stage=ReconStage.CONFIG_PULL,
            status=ReconStatus.FAILED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            error_message=str(e),
        )

    logger.info("[Stage 0] PASSED — all upstream data available")
    log_event("PROCESSING_COMPLETED", details={"stage": "stage0_config_pull", "status": "PASSED"})
    return StageReport(
        stage=ReconStage.CONFIG_PULL,
        status=ReconStatus.PASSED,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        metrics={"missing_count": 0.0},
    )
