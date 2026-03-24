"""ReconcileHandler: ServiceCLI handler for --operation reconcile."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import cast, override

from unified_trading_library import BaseModeHandler

from batch_live_reconciliation_service.engine.orchestrator import run_reconciliation

logger = logging.getLogger(__name__)


class ReconcileHandler(BaseModeHandler):
    """ServiceCLI handler for ``--operation reconcile``."""

    @override
    def validate_config(self) -> bool:
        return True

    @override
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
