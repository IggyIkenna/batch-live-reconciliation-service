"""DailyDeterminismHandler: ``--operation daily-determinism`` (P7.1-B).

Stage B of the paper-week determinism scheduler
(``deployment-service/terraform/gcp/paper_week_determinism_scheduler.tf``): the
DAILY T+1 trade-by-trade determinism recon. Reads the prior day's paper run +
its batch rerun ``RunManifest``s (from their configured ledger roots), runs
``run_daily_determinism_stage`` (keyed ``reconcile_day``, ε=0 verdict), and POSTs
the DETERMINISM verdict ``AlertEvent`` to alerting-service (P6.2 — INFO on ε=0,
CRITICAL on a determinism bug).

This is the scheduled wrapper over the pure engine
(``engine.daily_determinism_stage.run_daily_determinism_stage`` + the read side
``unified_trading_library.ledger.read_run_manifest``) and the verdict post
(``engine.recon_alert_client.post_recon_alert``). It owns no new recon logic.

SSOT: ``codex/09-strategy/operational/paper-batch-live-reconciliation.md`` §4.2+§6
Plan: ``plans/active/citadel_paper_batch_live_reconciliation_2026_06_19.md`` (P7.1-B + P2.6.2)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import cast, override

from unified_trading_library import BaseModeHandler
from unified_trading_library.ledger import read_run_manifest  # noqa: qg-deep-import

from batch_live_reconciliation_service.config import get_recon_config
from batch_live_reconciliation_service.engine.daily_determinism_stage import (
    run_daily_determinism_stage,
)
from batch_live_reconciliation_service.engine.recon_alert_client import post_recon_alert

logger = logging.getLogger(__name__)


class DailyDeterminismHandler(BaseModeHandler):
    """ServiceCLI handler for ``--operation daily-determinism`` (P7.1-B)."""

    @override
    def validate_config(self) -> bool:
        return True

    def _resolve_day(self) -> str:
        """Resolve the reconciled T+1 day — explicit ``--start-date`` or yesterday."""
        args = self.args
        start_date = cast("str | None", getattr(args, "start_date", None)) if args else None
        if start_date:
            _ = datetime.strptime(start_date, "%Y-%m-%d")  # validate format  # noqa: DTZ007
            return start_date
        return (datetime.now(UTC).date() - timedelta(days=1)).isoformat()

    @override
    async def run(self) -> dict[str, object]:
        """Run the daily T+1 determinism recon + POST the verdict.

        Honest no-op when no paper/batch ledger roots are configured (nothing to
        reconcile yet) — never a fabricated verdict. The CLI cron supplies the
        roots from the prior day's paper run + its batch rerun.
        """
        cfg = get_recon_config()
        if not cfg.paper_ledger_root or not cfg.batch_ledger_root:
            logger.info(
                "[daily-determinism] paper_ledger_root/batch_ledger_root unset — no run to reconcile (honest no-op)"
            )
            return {"status": "ok", "skipped": "no_run_configured"}

        day = self._resolve_day()
        day_start = datetime.fromisoformat(day).replace(tzinfo=UTC)
        day_end = day_start + timedelta(days=1)

        paper_manifest = read_run_manifest(cfg.paper_ledger_root)
        batch_manifest = read_run_manifest(cfg.batch_ledger_root)

        report, rollup = run_daily_determinism_stage(paper_manifest, batch_manifest, day_start, day_end)

        logger.info(
            "[daily-determinism] day=%s deterministic=%s bug=%s matched=%d rollups=%d",
            day,
            report.is_deterministic,
            report.determinism_bug_class.value,
            report.matched,
            len(rollup),
        )

        # P2.6.2 — POST the verdict (INFO on ε=0, CRITICAL on a determinism bug).
        await post_recon_alert(
            report,
            alerting_service_url=cfg.alerting_service_url,
            channel=cfg.recon_alert_channel,
        )

        if report.is_deterministic:
            return {"status": "ok", "deterministic": True}
        return {
            "status": "error",
            "message": "determinism_bug",
            "bug_class": report.determinism_bug_class.value,
        }
