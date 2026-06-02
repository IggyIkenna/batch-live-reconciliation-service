"""
T+1 Batch-Live Reconciliation Orchestrator.

Runs the 6-stage pipeline sequentially:
  Stage 0   — Config + Data Availability Check
  Stage 0.5 — Data Pipeline Reconciliation (instruments, MTDS, MDPS)
  Stage 1   — ML Reconciliation
  Stage 2   — Strategy Reconciliation
  Stage 3   — Execution Reconciliation
  Stage 4   — Agent Analysis
  Stage 5   — Consolidated Results Writer
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

from unified_api_contracts.alerting import RECON_GREEN_THRESHOLDS
from unified_trading_library import GCSEventSink, log_event, run_lifecycle, setup_events

from batch_live_reconciliation_service.config import ReconConfig, get_recon_config
from batch_live_reconciliation_service.models.deviation_thresholds import PAPER_LIVE_THRESHOLDS
from batch_live_reconciliation_service.models.recon_report import (
    ReconReport,
    ReconStatus,
)
from batch_live_reconciliation_service.stages.stage0_config_pull import run_stage0
from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
    run_data_pipeline_recon,
)
from batch_live_reconciliation_service.stages.stage1_ml_recon import run_stage1
from batch_live_reconciliation_service.stages.stage2_strategy_recon import run_stage2
from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3
from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import run_stage3b
from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import run_stage3c
from batch_live_reconciliation_service.stages.stage4_agent_analysis import run_stage4
from batch_live_reconciliation_service.stages.stage5_results_writer import run_stage5

logger = logging.getLogger(__name__)

_SERVICE_NAME = "batch-live-reconciliation-service"


def _setup_observability(config: ReconConfig) -> None:
    """Configure UEI event logging."""
    sink = GCSEventSink(
        project_id=config.gcp_project_id,
        bucket=config.events_bucket,
        service_name=_SERVICE_NAME,
    )
    setup_events(service_name=_SERVICE_NAME, mode="batch", sink=sink)


def run_reconciliation(date: str, dry_run: bool = False) -> ReconReport:
    """
    Run the full T+1 reconciliation pipeline for the given date.

    Args:
        date: YYYY-MM-DD date to reconcile
        dry_run: If True, skip all GCS reads/writes

    Returns:
        ReconReport with all stage results
    """
    config = get_recon_config()
    _setup_observability(config)

    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    with run_lifecycle(service_name=_SERVICE_NAME, details={"date": date, "run_id": run_id, "dry_run": dry_run}):
        log_event("STARTED", details={"date": date, "run_id": run_id, "dry_run": dry_run})
        logger.info("T+1 reconciliation starting: date=%s run_id=%s dry_run=%s", date, run_id, dry_run)

        report = ReconReport(
            date=date,
            run_id=run_id,
            started_at=started_at,
            status=ReconStatus.RUNNING,
        )

        # Stage 0: Config + availability check
        s0 = run_stage0(config, date, dry_run=dry_run)
        report.stages.append(s0)

        if s0.status == ReconStatus.FAILED:
            report.status = ReconStatus.FAILED
            report.completed_at = datetime.now(UTC)
            logger.error("Stage 0 failed — aborting pipeline")
            log_event("FAILED", details={"date": date, "stage": "stage0", "reason": s0.error_message})
            return report

        # Stage 0.5: Data pipeline reconciliation (instruments, MTDS, MDPS)
        s0_data = run_data_pipeline_recon(config, date, dry_run=dry_run)
        report.stages.append(s0_data)

        # Stage 1: ML reconciliation
        s1 = run_stage1(config, date, dry_run=dry_run)
        report.stages.append(s1)

        # Stage 2: Strategy reconciliation
        s2 = run_stage2(config, date, dry_run=dry_run)
        report.stages.append(s2)

        # Stage 3: Execution reconciliation
        s3 = run_stage3(config, date, dry_run=dry_run)
        report.stages.append(s3)

        # Emit BATCH_VS_LIVE_RECON_DRIFTED when batch-vs-live slippage exceeds the
        # most conservative archetype threshold from RECON_GREEN_THRESHOLDS (UAC SSOT).
        # Alerting-service picks this up via the BATCH_VS_LIVE_RECON_DRIFTED AlertRule.
        # Soak mode downgrades paging routing ("ALERT" → "ALERT_SUPPRESSED") so
        # alerting-service can suppress CRITICAL escalation during a soak window.
        _drift_routing = "ALERT_SUPPRESSED" if config.soak_mode else "ALERT"

        slippage_bps_s3 = s3.metrics.get("slippage_delta_bps", 0.0)
        drawdown_pct_s3 = s3.metrics.get("drawdown_pct", 0.0)
        fill_rate_s3 = s3.metrics.get("fill_rate", 1.0)
        _min_bps_threshold = min(float(t["bps_delta_max"]) for t in RECON_GREEN_THRESHOLDS.values())
        if slippage_bps_s3 > _min_bps_threshold:
            _breached_archetypes = [
                archetype
                for archetype, t in RECON_GREEN_THRESHOLDS.items()
                if slippage_bps_s3 > float(t["bps_delta_max"])
            ]
            log_event(
                "BATCH_VS_LIVE_RECON_DRIFTED",
                details={
                    "date": date,
                    "run_id": run_id,
                    "slippage_delta_bps": slippage_bps_s3,
                    "breached_archetypes": _breached_archetypes,
                    "thresholds": {k: str(v["bps_delta_max"]) for k, v in RECON_GREEN_THRESHOLDS.items()},
                    "soak_mode": config.soak_mode,
                    "routing": _drift_routing,
                },
            )

        # Green gate (operator: "build all three" — bps + drawdown + fill_rate).
        # A recon only stays GREEN when slippage_bps is within bps_delta_max (handled
        # above via stage failure), drawdown_pct is within the archetype drawdown_pct
        # bound, AND fill_rate is at/above the archetype fill_rate_min. The most
        # conservative archetype bound is the firm-wide green gate; any breach demotes
        # the run to FAILED and emits BATCH_VS_LIVE_RECON_DRIFTED for alerting.
        _max_drawdown_bound = min(float(t["drawdown_pct"]) for t in RECON_GREEN_THRESHOLDS.values())
        _min_fill_rate_bound = max(float(t["fill_rate_min"]) for t in RECON_GREEN_THRESHOLDS.values())
        _drawdown_breached = drawdown_pct_s3 > _max_drawdown_bound
        _fill_rate_breached = fill_rate_s3 < _min_fill_rate_bound
        if _drawdown_breached or _fill_rate_breached:
            _green_gate_breaches: list[str] = []
            if _drawdown_breached:
                _green_gate_breaches.append("drawdown_pct")
            if _fill_rate_breached:
                _green_gate_breaches.append("fill_rate")
            if s3.status != ReconStatus.FAILED:
                s3.status = ReconStatus.FAILED
            log_event(
                "BATCH_VS_LIVE_RECON_DRIFTED",
                details={
                    "date": date,
                    "run_id": run_id,
                    "drawdown_pct": drawdown_pct_s3,
                    "fill_rate": fill_rate_s3,
                    "drawdown_pct_max": _max_drawdown_bound,
                    "fill_rate_min": _min_fill_rate_bound,
                    "green_gate_breaches": _green_gate_breaches,
                    "soak_mode": config.soak_mode,
                    "routing": _drift_routing,
                },
            )

        # Stage 3b: Paper-vs-live reconciliation (pvl-p21a)
        s3b = run_stage3b(config, date, dry_run=dry_run)
        report.stages.append(s3b)

        # Emit BATCH_LIVE_RECON_DRIFT when paper-vs-live slippage exceeds 5bps threshold.
        # Alerting-service hooks this event for Telegram + PagerDuty routing.
        slippage_bps = s3b.metrics.get("slippage_delta_bps", 0.0)
        if slippage_bps > PAPER_LIVE_THRESHOLDS.slippage_delta_bps_max:
            log_event(
                "BATCH_LIVE_RECON_DRIFT",
                details={
                    "date": date,
                    "run_id": run_id,
                    "slippage_delta_bps": slippage_bps,
                    "threshold_bps": PAPER_LIVE_THRESHOLDS.slippage_delta_bps_max,
                    "stage3b_deviations": len(s3b.deviations),
                    "soak_mode": config.soak_mode,
                    "routing": "ALERT_SUPPRESSED" if config.soak_mode else "ALERT_AND_AUTO_DEMOTE",
                },
            )

        # Stage 3c: Batch-vs-paper reconciliation
        s3c = run_stage3c(config, date, dry_run=dry_run)
        report.stages.append(s3c)

        # Stage 4: Agent analysis (uses results from data pipeline + stages 1-3b-3c)
        s4 = run_stage4(config, date, stage_reports=[s0_data, s1, s2, s3, s3b, s3c], dry_run=dry_run)
        report.stages.append(s4)
        if s4.output_gcs_path:
            report.agent_report_gcs_path = s4.output_gcs_path

        # Determine overall status
        failed_stages = [s for s in report.stages if s.status == ReconStatus.FAILED]
        report.status = ReconStatus.FAILED if failed_stages else ReconStatus.PASSED

        # Stage 5: Write consolidated results
        s5 = run_stage5(config, report, dry_run=dry_run)
        report.stages.append(s5)
        if s5.output_gcs_path:
            report.summary_gcs_path = s5.output_gcs_path

        report.completed_at = datetime.now(UTC)

        total_deviations = report.total_deviations
        logger.info(
            "T+1 reconciliation complete: date=%s status=%s total_deviations=%d",
            date,
            report.status.value,
            total_deviations,
        )

        if report.status == ReconStatus.PASSED:
            log_event(
                "STOPPED",
                details={"date": date, "run_id": run_id, "total_deviations": total_deviations},
            )
        else:
            failed_stages_str = ", ".join(s.value for s in report.failed_stages)
            log_event(
                "FAILED",
                details={
                    "date": date,
                    "run_id": run_id,
                    "total_deviations": total_deviations,
                    "failed_stages": failed_stages_str,
                },
            )

        return report
