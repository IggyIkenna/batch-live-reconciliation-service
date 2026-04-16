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

from unified_trading_library import GCSEventSink, log_event, setup_events

from batch_live_reconciliation_service.config import ReconConfig, get_recon_config
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

    # Stage 4: Agent analysis (uses results from data pipeline + stages 1-3)
    s4 = run_stage4(config, date, stage_reports=[s0_data, s1, s2, s3], dry_run=dry_run)
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
