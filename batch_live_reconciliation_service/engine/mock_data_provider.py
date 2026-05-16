"""Mock data provider for batch-live-reconciliation-service.

In mock mode (CLOUD_MOCK_MODE=true), loads mock data from execution-service
and strategy-service seeds, runs REAL deviation detection logic using the
service's threshold definitions, and writes a reconciliation report.

Reads upstream from:
    .local-dev-cache/mock-seed/execution-service/
    .local-dev-cache/mock-seed/strategy-service/

Writes output to:
    .local-dev-cache/mock-seed/batch-live-reconciliation-service/
"""

from __future__ import annotations

import json
import logging
import os  # noqa: qg-os-env — mock dev infrastructure only; not production config
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from batch_live_reconciliation_service.models.deviation_thresholds import (
    EXECUTION_THRESHOLDS,
    ML_THRESHOLDS,
)
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconStage,
    ReconStatus,
)

logger = logging.getLogger(__name__)

SERVICE_NAME: Final[str] = "batch-live-reconciliation-service"
UPSTREAM_SERVICES: Final[list[str]] = ["execution-service", "strategy-service"]
LAYER: Final[int] = 7


def _get_workspace_root() -> Path:
    """Resolve workspace root from env or heuristics."""
    workspace: str | None = os.environ.get("WORKSPACE_ROOT")  # noqa: qg-os-env
    if workspace:
        return Path(workspace)
    return Path(__file__).resolve().parents[3]


def _get_seed_base(service: str = SERVICE_NAME) -> Path:
    """Return the seed data directory for a service."""
    return _get_workspace_root() / ".local-dev-cache" / "mock-seed" / service


def _upstream_available() -> bool:
    """Check if any upstream seed data is available."""
    return any((_get_seed_base(svc) / ".seed-complete").exists() for svc in UPSTREAM_SERVICES)


def _load_json(path: Path) -> list[dict[str, object]] | dict[str, object]:
    """Load JSON from path, return empty list if missing."""
    if path.exists():
        raw: list[dict[str, object]] | dict[str, object] = cast("list[dict[str, object]]", json.loads(path.read_text()))
        return raw
    return []


def _run_ml_recon(
    predictions_path: Path,
) -> tuple[ReconStatus, list[DeviationRecord], dict[str, float]]:
    """Stage 1: ML reconciliation -- check signal direction match rate."""
    predictions = _load_json(predictions_path)
    if not isinstance(predictions, list) or not predictions:
        return ReconStatus.SKIPPED, [], {}

    # In mock mode: simulate a batch-vs-live direction match rate
    total = len(predictions)
    matching = sum(1 for p in predictions if float(str(p.get("confidence") or "0")) > 0.5)
    match_rate = matching / total if total > 0 else 0.0

    deviations: list[DeviationRecord] = []
    if match_rate < ML_THRESHOLDS.signal_direction_match_rate_min:
        deviations.append(
            DeviationRecord(
                metric_name="signal_direction_match_rate",
                stage=ReconStage.ML_RECON,
                actual_value=match_rate,
                threshold=ML_THRESHOLDS.signal_direction_match_rate_min,
                direction="below",
                description=(
                    f"Signal direction match rate {match_rate:.2%}"
                    f" below threshold {ML_THRESHOLDS.signal_direction_match_rate_min:.2%}"
                ),
            )
        )

    status = ReconStatus.FAILED if deviations else ReconStatus.PASSED
    metrics = {"signal_direction_match_rate": match_rate, "total_predictions": float(total)}
    return status, deviations, metrics


def _run_execution_recon(
    fills_path: Path,
) -> tuple[ReconStatus, list[DeviationRecord], dict[str, float]]:
    """Stage 3: Execution reconciliation -- check fill rate and slippage."""
    fills = _load_json(fills_path)
    if not isinstance(fills, list) or not fills:
        return ReconStatus.SKIPPED, [], {}

    total_fills = len(fills)
    slippages = [float(str(f.get("slippage_bps", "0"))) for f in fills]
    avg_slippage = sum(slippages) / len(slippages) if slippages else 0.0

    deviations: list[DeviationRecord] = []
    if avg_slippage > EXECUTION_THRESHOLDS.slippage_delta_bps_max:
        deviations.append(
            DeviationRecord(
                metric_name="avg_slippage_bps",
                stage=ReconStage.EXECUTION_RECON,
                actual_value=avg_slippage,
                threshold=EXECUTION_THRESHOLDS.slippage_delta_bps_max,
                direction="above",
                description=(
                    f"Average slippage {avg_slippage:.1f} bps exceeds"
                    f" threshold {EXECUTION_THRESHOLDS.slippage_delta_bps_max:.1f} bps"
                ),
            )
        )

    status = ReconStatus.FAILED if deviations else ReconStatus.PASSED
    metrics = {"total_fills": float(total_fills), "avg_slippage_bps": avg_slippage}
    return status, deviations, metrics


def run_mock_pipeline() -> int:
    """Run the mock pipeline for batch-live-reconciliation-service.

    1. Load upstream data from execution + strategy services
    2. Run REAL deviation detection against configured thresholds
    3. Write reconciliation report to seed directory

    Returns:
        Exit code (0 = success, 1 = failure).
    """
    seed_base = _get_seed_base()
    marker = seed_base / ".seed-complete"

    if marker.exists():
        logger.info(
            "MOCK MODE: Seed data already present at %s -- skipping generation",
            seed_base,
        )
        return 0

    upstream_ok = _upstream_available()
    logger.info("MOCK MODE: upstream available=%s", upstream_ok)

    # Run reconciliation stages
    exec_seed = _get_seed_base("execution-service")
    ml_seed = _get_seed_base("ml-inference-service")

    all_deviations: list[dict[str, object]] = []
    stage_results: list[dict[str, object]] = []

    # Stage 1: ML recon
    ml_status, ml_devs, ml_metrics = _run_ml_recon(ml_seed / "predictions" / "predictions.json")
    stage_results.append(
        {
            "stage": ReconStage.ML_RECON,
            "status": str(ml_status),
            "metrics": ml_metrics,
            "deviation_count": len(ml_devs),
        }
    )
    for d in ml_devs:
        all_deviations.append(d.model_dump())
    logger.info("MOCK MODE: ML recon: %s (%d deviations)", ml_status, len(ml_devs))

    # Stage 3: Execution recon
    exec_status, exec_devs, exec_metrics = _run_execution_recon(exec_seed / "fills" / "fills.json")
    stage_results.append(
        {
            "stage": ReconStage.EXECUTION_RECON,
            "status": str(exec_status),
            "metrics": exec_metrics,
            "deviation_count": len(exec_devs),
        }
    )
    for d in exec_devs:
        all_deviations.append(d.model_dump())
    logger.info("MOCK MODE: Execution recon: %s (%d deviations)", exec_status, len(exec_devs))

    # Build consolidated report
    overall_status = "PASSED"
    for sr in stage_results:
        if sr["status"] == str(ReconStatus.FAILED):
            overall_status = "FAILED"
            break

    report = {
        "date": datetime.now(UTC).strftime("%Y-%m-%d"),
        "run_id": "mock-recon-run",
        "status": overall_status,
        "started_at": datetime.now(UTC).isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "stages": stage_results,
        "deviations": all_deviations,
        "total_deviations": len(all_deviations),
    }

    # Write report
    seed_base.mkdir(parents=True, exist_ok=True)
    report_dir = seed_base / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    _ = (report_dir / "recon_report.json").write_text(json.dumps(report, indent=2, default=str))

    marker_data = json.dumps(
        {
            "service": SERVICE_NAME,
            "completed_at": datetime.now(UTC).isoformat(),
            "layer": LAYER,
            "mock_mode": True,
            "upstream_available": upstream_ok,
            "ran_real_deviation_detection": True,
            "overall_status": overall_status,
            "deviation_count": len(all_deviations),
        }
    )
    _ = marker.write_text(marker_data)
    logger.info("MOCK MODE: %s pipeline complete -- %s", SERVICE_NAME, overall_status)
    return 0
