"""
CLI entry point for batch-live-reconciliation-service.

Uses ServiceBootstrap from unified-trading-library to handle all
infrastructure boilerplate. The ReconcileHandler runs the T+1
batch-live reconciliation pipeline.

Usage:
    python -m batch_live_reconciliation_service --operation reconcile --mode batch \
        --start-date 2026-03-09 --end-date 2026-03-09
    python -m batch_live_reconciliation_service --operation reconcile --mode batch \
        --start-date 2026-03-09 --end-date 2026-03-09 --dry-run
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime, timedelta
from typing import cast

from unified_trading_library import BaseModeHandler, ServiceBootstrap

from batch_live_reconciliation_service.orchestrator import run_reconciliation

logger = logging.getLogger(__name__)


class ReconcileHandler(BaseModeHandler):
    """ServiceCLI handler for ``--operation reconcile``."""

    def validate_config(self) -> bool:
        return True

    async def run(self) -> dict[str, object]:
        """Run T+1 reconciliation for the date range."""
        args = self.args
        if args is None:
            return {"status": "error", "message": "No args provided"}

        start_date: str | None = cast("str | None", getattr(args, "start_date", None))
        dry_run: bool = cast(bool, getattr(args, "dry_run", False))

        if start_date:
            _ = datetime.strptime(start_date, "%Y-%m-%d")  # validate format
            date = start_date
        else:
            date = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()

        logger.info("Starting reconciliation: date=%s dry_run=%s", date, dry_run)

        report = run_reconciliation(date=date, dry_run=dry_run)

        if report.status.value == "passed":
            logger.info("Reconciliation PASSED -- %d total deviations", report.total_deviations)
            return {"status": "ok"}
        else:
            logger.error(
                "Reconciliation FAILED -- %d deviations, failed stages: %s",
                report.total_deviations,
                [s.value for s in report.failed_stages],
            )
            return {"status": "error", "message": "reconciliation_failed"}


def _add_recon_args(parser: argparse.ArgumentParser) -> None:
    """Add reconciliation-specific CLI arguments."""
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )


_OPERATIONS: dict[str, type[BaseModeHandler]] = {
    "reconcile": ReconcileHandler,
}


def _get_mock_pipeline() -> int:
    from batch_live_reconciliation_service.engine.mock_data_provider import run_mock_pipeline

    return run_mock_pipeline()


def main() -> None:
    """Main CLI entry point -- ServiceBootstrap handles all infrastructure.

    SERVICE_EVENT: STARTED
    SERVICE_EVENT: STOPPED
    SERVICE_EVENT: FAILED
    """
    ServiceBootstrap(
        service_name="batch-live-reconciliation-service",
        operations=_OPERATIONS,
        config={},
        modes=["batch"],
        description="T+1 Batch-Live Reconciliation -- nightly pipeline replay and deviation analysis",
        add_category_arg=False,
        extra_args_fn=_add_recon_args,
        mock_pipeline_fn=_get_mock_pipeline,
    ).run()


if __name__ == "__main__":
    main()
