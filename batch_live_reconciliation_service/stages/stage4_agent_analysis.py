"""
Stage 4 — Agent Analysis.

Dispatches reconciliation reports to trading-agent-service for root-cause
analysis and improvement suggestions. Publishes agent report to GCS and
emits a PubSub alert to alerting-service → Slack #trading-recon.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from unified_cloud_interface import get_storage_client
from unified_events_interface import log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.recon_report import (
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


def _build_agent_prompt(
    date: str,
    stage_reports: list[StageReport],
) -> str:
    """Build the analysis prompt for the trading-agent-service."""
    total_deviations = sum(len(s.deviations) for s in stage_reports)

    lines: list[str] = [
        f"# T+1 Reconciliation Analysis — {date}",
        "",
        f"Total deviations detected: {total_deviations}",
        "",
    ]

    for stage_report in stage_reports:
        lines.append(f"## {stage_report.stage.value}")
        lines.append(f"Status: {stage_report.status.value}")
        if stage_report.deviations:
            lines.append("Deviations:")
            for d in stage_report.deviations:
                lines.append(f"  - {d.metric_name}: {d.description}")
        if stage_report.metrics:
            lines.append("Metrics:")
            for k, v in stage_report.metrics.items():
                lines.append(f"  - {k}: {v:.4f}")
        lines.append("")

    lines += [
        "Please:",
        "1. Identify the largest deviations and their likely root causes",
        "2. Classify: data quality issue, model drift, config change, or execution slippage",
        "3. Suggest concrete improvements for each deviation category",
        "4. Flag any deviations that require immediate operator review",
    ]
    return "\n".join(lines)


def _write_agent_report(
    bucket: str,
    date: str,
    report_content: str,
    dry_run: bool,
) -> str:
    """Write agent report markdown to GCS. Returns GCS URI."""
    blob_path = f"t1-recon/recon/agent_report_{date}.md"
    gcs_uri = f"gs://{bucket}/{blob_path}"

    if dry_run:
        logger.info("[Stage 4] DRY RUN — would write agent report to %s", gcs_uri)
        return gcs_uri

    client = get_storage_client()
    client.upload_bytes(
        bucket=bucket,
        blob_path=blob_path,
        data=report_content.encode("utf-8"),
    )
    logger.info("[Stage 4] Agent report written to %s", gcs_uri)
    return gcs_uri


def run_stage4(
    config: ReconConfig,
    date: str,
    stage_reports: list[StageReport],
    dry_run: bool = False,
) -> StageReport:
    """
    Run Stage 4: Agent Analysis.

    Args:
        config: ReconConfig instance
        date: YYYY-MM-DD date string
        stage_reports: Reports from stages 0-3
        dry_run: If True, skip GCS writes

    Returns:
        StageReport for Stage 4
    """
    started_at = datetime.now(UTC)
    log_event("PROCESSING_STARTED", details={"stage": "stage4_agent_analysis", "date": date})
    logger.info("[Stage 4] Agent analysis for date=%s", date)

    total_deviations = sum(len(s.deviations) for s in stage_reports)
    logger.info("[Stage 4] Analysing %d total deviations across %d stages", total_deviations, len(stage_reports))

    try:
        # Build analysis prompt
        prompt = _build_agent_prompt(date, stage_reports)

        # In production: dispatch to trading-agent-service via its task API
        # For now: write a structured markdown report from the deviation data directly
        # (trading-agent-service integration is wired in Phase 6)
        agent_report = f"# Agent Analysis Report — {date}\n\n"
        agent_report += f"**Generated:** {datetime.now(UTC).isoformat()}\n\n"
        agent_report += f"**Total Deviations:** {total_deviations}\n\n"

        for stage_report in stage_reports:
            if stage_report.deviations:
                agent_report += f"## {stage_report.stage.value} ({len(stage_report.deviations)} deviations)\n\n"
                for d in stage_report.deviations:
                    agent_report += f"- **{d.metric_name}**: {d.description}\n"
                    agent_report += f"  - Actual: `{d.actual_value:.4f}`, Threshold: `{d.threshold}`\n"
                agent_report += "\n"

        if total_deviations == 0:
            agent_report += "## Summary\n\nAll metrics within acceptable thresholds. No action required.\n"
        else:
            agent_report += "## Summary\n\nDeviations detected above alert thresholds. "
            agent_report += "Review individual stage reports and investigate root causes.\n"

        gcs_uri = _write_agent_report(config.recon_bucket, date, agent_report, dry_run)

        log_event(
            "PROCESSING_COMPLETED",
            details={"stage": "stage4_agent_analysis", "status": "PASSED", "gcs_uri": gcs_uri},
        )
        return StageReport(
            stage=ReconStage.AGENT_ANALYSIS,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            output_gcs_path=gcs_uri,
            metrics={"total_deviations_analysed": float(total_deviations)},
        )

    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError) as e:
        logger.exception("[Stage 4] Agent analysis failed: %s", e)
        log_event("PROCESSING_COMPLETED", details={"stage": "stage4_agent_analysis", "status": "FAILED"})
        return StageReport(
            stage=ReconStage.AGENT_ANALYSIS,
            status=ReconStatus.FAILED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            error_message=str(e),
        )
