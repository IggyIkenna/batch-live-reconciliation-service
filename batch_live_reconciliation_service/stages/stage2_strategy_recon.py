"""
Stage 2 — Strategy Reconciliation.

Compares batch T+1 strategy outputs vs live strategy events.

Batch:  t1-recon/events/{date}/strategy-service/
Live:   live/events/{date}/strategy-service/

Deviation metrics:
  - instruction_alignment_pct   (< 85% → alert)
  - benchmark_pnl_delta         (> 2% of notional → alert)
  - position_snapshot_delta     (> 1 unit per instrument → alert)
  - var_delta_pct               (> 10% → alert)
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import cast

from unified_cloud_interface import get_storage_client
from unified_events_interface import log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.deviation_thresholds import STRATEGY_THRESHOLDS
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


def _load_events(bucket: str, prefix: str) -> list[dict[str, object]]:
    """Load NDJSON event files from a GCS prefix."""
    client = get_storage_client()
    events: list[dict[str, object]] = []
    try:
        bucket_obj = client.bucket(bucket)
        blobs = list(bucket_obj.list_blobs(prefix=prefix))
        for blob in blobs:
            if not str(blob.name).endswith((".ndjson", ".jsonl")):
                continue
            raw = client.download_bytes(bucket=bucket, blob_path=str(blob.name))
            for line in raw.decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    events.append(cast(dict[str, object], json.loads(line)))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
        logger.warning("Error loading events from gs://%s/%s: %s", bucket, prefix, e)
    return events


def _compute_metrics(
    batch_events: list[dict[str, object]],
    live_events: list[dict[str, object]],
) -> dict[str, float]:
    """Compute strategy reconciliation metrics."""
    if not live_events:
        return {
            "instruction_alignment_pct": 0.0,
            "benchmark_pnl_delta": 0.0,
            "position_snapshot_delta": 0.0,
            "var_delta_pct": 0.0,
            "batch_event_count": float(len(batch_events)),
            "live_event_count": 0.0,
        }

    # Index instruction events by instrument_id + side
    def _key(ev: dict[str, object]) -> str:
        inst = str(ev.get("instrument_id", ""))
        side = str(ev.get("side", ""))
        return f"{inst}:{side}"

    batch_instructions = {_key(e): e for e in batch_events if e.get("event_type") == "INSTRUCTION"}
    live_instructions = {_key(e): e for e in live_events if e.get("event_type") == "INSTRUCTION"}

    matched = set(batch_instructions) & set(live_instructions)
    alignment = len(matched) / max(len(live_instructions), 1)

    # P&L delta from position snapshots
    batch_pnl = sum(
        float(cast(float, e.get("unrealized_pnl", 0.0)))
        for e in batch_events if e.get("event_type") == "POSITION_SNAPSHOT"
    )
    live_pnl = sum(
        float(cast(float, e.get("unrealized_pnl", 0.0)))
        for e in live_events if e.get("event_type") == "POSITION_SNAPSHOT"
    )
    notional = abs(live_pnl) or 1.0
    pnl_delta = abs(batch_pnl - live_pnl) / notional

    # Max position deviation
    batch_positions: dict[str, float] = {
        str(e.get("instrument_id", "")): float(cast(float, e.get("net_position", 0.0)))
        for e in batch_events if e.get("event_type") == "POSITION_SNAPSHOT"
    }
    live_positions: dict[str, float] = {
        str(e.get("instrument_id", "")): float(cast(float, e.get("net_position", 0.0)))
        for e in live_events if e.get("event_type") == "POSITION_SNAPSHOT"
    }
    all_insts = set(batch_positions) | set(live_positions)
    pos_deltas = [abs(batch_positions.get(i, 0.0) - live_positions.get(i, 0.0)) for i in all_insts]
    max_pos_delta = max(pos_deltas) if pos_deltas else 0.0

    # VaR delta
    batch_var = sum(
        float(cast(float, e.get("var_1d", 0.0)))
        for e in batch_events if e.get("event_type") == "RISK_SNAPSHOT"
    )
    live_var = sum(
        float(cast(float, e.get("var_1d", 0.0)))
        for e in live_events if e.get("event_type") == "RISK_SNAPSHOT"
    )
    var_delta = abs(batch_var - live_var) / max(abs(live_var), 1.0)

    return {
        "instruction_alignment_pct": alignment,
        "benchmark_pnl_delta": pnl_delta,
        "position_snapshot_delta": max_pos_delta,
        "var_delta_pct": var_delta,
        "batch_event_count": float(len(batch_events)),
        "live_event_count": float(len(live_events)),
    }


def _check_deviations(metrics: dict[str, float]) -> list[DeviationRecord]:
    t = STRATEGY_THRESHOLDS
    deviations: list[DeviationRecord] = []

    if metrics["instruction_alignment_pct"] < t.instruction_alignment_pct_min:
        deviations.append(DeviationRecord(
            metric_name="instruction_alignment_pct",
            stage=ReconStage.STRATEGY_RECON,
            actual_value=metrics["instruction_alignment_pct"],
            threshold=t.instruction_alignment_pct_min,
            direction="below",
            description=(
                f"Instruction alignment {metrics['instruction_alignment_pct']:.1%} "
                f"< {t.instruction_alignment_pct_min:.0%}"
            ),
        ))

    if metrics["benchmark_pnl_delta"] > t.benchmark_pnl_delta_max:
        deviations.append(DeviationRecord(
            metric_name="benchmark_pnl_delta",
            stage=ReconStage.STRATEGY_RECON,
            actual_value=metrics["benchmark_pnl_delta"],
            threshold=t.benchmark_pnl_delta_max,
            direction="above",
            description=(
                f"Benchmark P&L delta {metrics['benchmark_pnl_delta']:.1%} "
                f"> {t.benchmark_pnl_delta_max:.0%} of notional"
            ),
        ))

    if metrics["position_snapshot_delta"] > t.position_snapshot_delta_max:
        deviations.append(DeviationRecord(
            metric_name="position_snapshot_delta",
            stage=ReconStage.STRATEGY_RECON,
            actual_value=metrics["position_snapshot_delta"],
            threshold=t.position_snapshot_delta_max,
            direction="above",
            description=(
                f"Max position delta {metrics['position_snapshot_delta']:.2f} "
                f"> {t.position_snapshot_delta_max} units"
            ),
        ))

    if metrics["var_delta_pct"] > t.var_delta_pct_max:
        deviations.append(DeviationRecord(
            metric_name="var_delta_pct",
            stage=ReconStage.STRATEGY_RECON,
            actual_value=metrics["var_delta_pct"],
            threshold=t.var_delta_pct_max,
            direction="above",
            description=(
                f"VaR delta {metrics['var_delta_pct']:.1%} > {t.var_delta_pct_max:.0%}"
            ),
        ))

    return deviations


def run_stage2(config: ReconConfig, date: str, dry_run: bool = False) -> StageReport:
    """Run Stage 2: Strategy Reconciliation."""
    started_at = datetime.now(UTC)
    log_event("PROCESSING_STARTED", details={"stage": "stage2_strategy_recon", "date": date})
    logger.info("[Stage 2] Strategy reconciliation for date=%s", date)

    if dry_run:
        return StageReport(
            stage=ReconStage.STRATEGY_RECON,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    batch_events = _load_events(config.events_bucket, f"t1-recon/events/{date}/strategy-service/")
    live_events = _load_events(config.events_bucket, f"live/events/{date}/strategy-service/")

    logger.info("[Stage 2] %d batch, %d live events", len(batch_events), len(live_events))

    metrics = _compute_metrics(batch_events, live_events)
    deviations = _check_deviations(metrics)
    status = ReconStatus.PASSED if not deviations else ReconStatus.FAILED

    for d in deviations:
        logger.warning("[Stage 2] deviation: %s", d.description)

    log_event(
        "PROCESSING_COMPLETED",
        details={"stage": "stage2_strategy_recon", "status": status.value, "deviations": len(deviations)},
    )
    return StageReport(
        stage=ReconStage.STRATEGY_RECON,
        status=status,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        deviations=deviations,
        metrics=metrics,
    )
