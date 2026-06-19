"""Unit tests for the ``daily-determinism`` CLI handler (P7.1-B + P2.6.2).

Covers ``cli.handlers.daily_determinism_handler.DailyDeterminismHandler`` — the
scheduled stage that reads the paper + batch ``RunManifest``s, runs the
determinism stage, and POSTs the verdict. Asserts: honest no-op when the ledger
roots are unconfigured; the deterministic (ε=0) path returns ok + posts INFO;
a determinism-bug path returns error + still posts the verdict.

SSOT: plans/active/citadel_paper_batch_live_reconciliation_2026_06_19.md (P7.1-B / P2.6.2).
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from unified_api_contracts.internal.reconciliation import (
    DailyReconReport,
    DeterminismBugClass,
    ReconVerdictType,
)

from batch_live_reconciliation_service.cli.handlers.daily_determinism_handler import (
    DailyDeterminismHandler,
)

_DAY = datetime(2026, 5, 2, tzinfo=UTC)


def _report(*, deterministic: bool) -> DailyReconReport:
    return DailyReconReport(
        verdict_type=ReconVerdictType.DETERMINISM,
        run_a_id="paper-1",
        run_b_id="batch-1",
        window_start=_DAY,
        window_end=datetime(2026, 5, 3, tzinfo=UTC),
        total_trades_a=3,
        total_trades_b=3,
        matched=3,
        unmatched_a=0,
        unmatched_b=0,
        deviations=(),
        is_deterministic=deterministic,
        determinism_bug_class=(DeterminismBugClass.NONE if deterministic else DeterminismBugClass.FILL_MODEL_DRIFT),
        mean_fill_price_delta_bps=Decimal("0"),
        p99_fill_price_delta_bps=Decimal("0"),
    )


def _handler(args: argparse.Namespace) -> DailyDeterminismHandler:
    handler = DailyDeterminismHandler.__new__(DailyDeterminismHandler)
    handler.args = args  # pyright: ignore[reportAttributeAccessIssue]
    return handler


def _cfg(*, paper: str, batch: str) -> MagicMock:
    cfg = MagicMock()
    cfg.paper_ledger_root = paper
    cfg.batch_ledger_root = batch
    cfg.alerting_service_url = "http://alerting"
    cfg.recon_alert_channel = "#uts-live-alerts"
    return cfg


def test_no_run_configured_is_honest_noop() -> None:
    handler = _handler(argparse.Namespace(start_date="2026-05-02"))
    with patch(
        "batch_live_reconciliation_service.cli.handlers.daily_determinism_handler.get_recon_config",
        return_value=_cfg(paper="", batch=""),
    ):
        result = asyncio.run(handler.run())
    assert result["status"] == "ok"
    assert result["skipped"] == "no_run_configured"


def test_deterministic_path_returns_ok_and_posts() -> None:
    handler = _handler(argparse.Namespace(start_date="2026-05-02"))
    mod = "batch_live_reconciliation_service.cli.handlers.daily_determinism_handler"
    with (
        patch(f"{mod}.get_recon_config", return_value=_cfg(paper="gs://p/", batch="gs://b/")),
        patch(f"{mod}.read_run_manifest", return_value=MagicMock()),
        patch(
            f"{mod}.run_daily_determinism_stage",
            return_value=(_report(deterministic=True), []),
        ),
        patch(f"{mod}.post_recon_alert", new=AsyncMock()) as mock_post,
    ):
        result = asyncio.run(handler.run())
    assert result["status"] == "ok"
    assert result["deterministic"] is True
    mock_post.assert_awaited_once()


def test_determinism_bug_returns_error_and_still_posts() -> None:
    handler = _handler(argparse.Namespace(start_date="2026-05-02"))
    mod = "batch_live_reconciliation_service.cli.handlers.daily_determinism_handler"
    with (
        patch(f"{mod}.get_recon_config", return_value=_cfg(paper="gs://p/", batch="gs://b/")),
        patch(f"{mod}.read_run_manifest", return_value=MagicMock()),
        patch(
            f"{mod}.run_daily_determinism_stage",
            return_value=(_report(deterministic=False), []),
        ),
        patch(f"{mod}.post_recon_alert", new=AsyncMock()) as mock_post,
    ):
        result = asyncio.run(handler.run())
    assert result["status"] == "error"
    assert result["bug_class"] == DeterminismBugClass.FILL_MODEL_DRIFT.value
    mock_post.assert_awaited_once()
