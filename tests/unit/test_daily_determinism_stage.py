"""Unit tests for the daily T+1 determinism recon stage + Slack verdict (P4.2 + P6).

Credential-free. Mocks the GCS ledger read with in-memory ``TradeFillRecord``
fixtures and proves:
  - ε=0 paper≡batch → ``is_deterministic`` True (no rollup deviations);
  - a fill-price drift → ``FILL_MODEL_DRIFT`` bug + populated per-(venue,instrument)
    rollup;
  - the report→AlertEvent mapping yields INFO on ε=0 and CRITICAL on a bug;
  - the HTTP POST is invoked with the AlertEvent JSON (mocked transport).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import (
    DeterminismBugClass,
    FillModel,
    ReconVerdictType,
    RunManifest,
    TradeFillRecord,
)
from unified_api_contracts.internal.reconciliation import TradingMode

from batch_live_reconciliation_service.engine.daily_determinism_stage import run_daily_determinism_stage
from batch_live_reconciliation_service.engine.recon_alert_client import (
    build_recon_alert_event,
    post_recon_alert,
)

_DAY_START = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
_DAY_END = datetime(2026, 6, 20, 0, 0, 0, tzinfo=UTC)
_TICK = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
_CHANNEL = "#uts-live-alerts"


def _fill(
    trade_key: str, *, fill_price: str = "100.0", fees: str = "0.10", recon_excluded: bool = False
) -> TradeFillRecord:
    return TradeFillRecord(
        trade_key=trade_key,
        instrument_key="binance:perp:BTC-USDT",
        strategy_instruction_id=f"instr-{trade_key}",
        tick_timestamp=_TICK,
        venue="binance",
        side="BUY",
        qty=Decimal("1.0"),
        fill_price=Decimal(fill_price),
        fees_in_quote=Decimal(fees),
        fill_model=FillModel.BENCHMARK,
        recon_excluded=recon_excluded,
    )


def _manifest(run_id: str, *, mode: TradingMode, parent: str | None = None) -> RunManifest:
    return RunManifest(
        run_id=run_id,
        window_start=_DAY_START,
        window_end=_DAY_END,
        mode=mode,
        client_id="odum-uk",
        strategy_ids=("strat-1",),
        code_shas={"strategy-service": "abc123"},
        market_data_days=("2026-06-19",),
        feature_group_versions={"delta_one": 1},
        captured_tick_stream="gs://ticks/run/",
        fill_model=FillModel.BENCHMARK,
        ledger_root=f"gs://ledgers/{run_id}/",
        parent_run_id=parent,
    )


def _run_stage(paper_fills: list[TradeFillRecord], batch_fills: list[TradeFillRecord]) -> tuple[object, list[object]]:
    paper = _manifest("paper-1", mode=TradingMode.PAPER)
    batch = _manifest("batch-1", mode=TradingMode.BATCH, parent="paper-1")
    with patch(
        "batch_live_reconciliation_service.engine.daily_determinism_stage.load_instruction_ledger_fills",
        side_effect=[paper_fills, batch_fills],
    ):
        return run_daily_determinism_stage(paper, batch, _DAY_START, _DAY_END)


def test_identical_ledgers_are_deterministic() -> None:
    """ε=0 paper≡batch → is_deterministic True, NONE bug, no rollup deviations."""
    fills = [_fill("t1"), _fill("t2", fill_price="200.0")]
    report, rollup = _run_stage(fills, list(fills))

    assert report.verdict_type == ReconVerdictType.DETERMINISM
    assert report.is_deterministic is True
    assert report.determinism_bug_class == DeterminismBugClass.NONE
    assert report.matched == 2
    assert rollup == []


def test_fill_price_drift_is_fill_model_drift_bug() -> None:
    """A matched trade with a fill-price drift → FILL_MODEL_DRIFT + populated rollup."""
    paper = [_fill("t1", fill_price="100.0")]
    batch = [_fill("t1", fill_price="105.0")]  # +500 bps drift on the same trade_key
    report, rollup = _run_stage(paper, batch)

    assert report.is_deterministic is False
    assert report.determinism_bug_class == DeterminismBugClass.FILL_MODEL_DRIFT

    # Per-(venue, instrument) rollup is populated and carries instrument_id.
    instrument_records = [r for r in rollup if r.metric_name == "determinism_fill_price_delta_bps"]
    assert len(instrument_records) == 1
    rec = instrument_records[0]
    assert rec.instrument_id == "binance:perp:BTC-USDT"
    assert rec.actual_value == pytest.approx(500.0, abs=1.0)

    # A SLIPPAGE PnLFactor rollup is produced (fill-price divergence axis).
    factor_records = [r for r in rollup if r.metric_name.startswith("determinism_factor_")]
    assert any("slippage" in r.metric_name for r in factor_records)


def test_recon_excluded_fill_skipped_from_matching() -> None:
    """A manual-trade second-booking-path fill (recon_excluded=True) is excluded
    from reconcile_day()'s matching -- present in the paper ledger only (no batch
    counterpart, as an exchange-outage/OTC/not-yet-cleared fill genuinely has none)
    must NOT surface as an unmatched-fill deviation."""
    paper = [_fill("t1"), _fill("manual-otc-1", recon_excluded=True)]
    batch = [_fill("t1")]  # no counterpart for the recon_excluded fill -- by design
    report, rollup = _run_stage(paper, batch)

    assert report.is_deterministic is True
    assert report.determinism_bug_class == DeterminismBugClass.NONE
    assert report.matched == 1
    assert report.unmatched_a == 0
    assert rollup == []


def test_alert_event_info_on_epsilon_zero() -> None:
    """ε=0 determinism pass → AlertEvent severity INFO with execution-alpha summary."""
    fills = [_fill("t1"), _fill("t2", fill_price="200.0")]
    report, _ = _run_stage(fills, list(fills))

    event = build_recon_alert_event(report, channel=_CHANNEL)
    assert event.severity == "INFO"
    assert _CHANNEL in event.message
    assert "GREEN" in event.message


def test_alert_event_critical_on_determinism_bug() -> None:
    """A determinism bug → AlertEvent severity CRITICAL."""
    report, _ = _run_stage([_fill("t1", fill_price="100.0")], [_fill("t1", fill_price="105.0")])

    event = build_recon_alert_event(report, channel=_CHANNEL)
    assert event.severity == "CRITICAL"
    assert "DETERMINISM BUG" in event.message
    assert DeterminismBugClass.FILL_MODEL_DRIFT.value in event.message


@pytest.mark.asyncio
async def test_post_recon_alert_posts_json() -> None:
    """post_recon_alert POSTs the AlertEvent JSON to the alerting-service ingress."""
    report, _ = _run_stage([_fill("t1", fill_price="100.0")], [_fill("t1", fill_price="105.0")])

    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_resp)
    ctx.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.post = MagicMock(return_value=ctx)

    event = await post_recon_alert(
        report,
        alerting_service_url="http://alerting.local",
        channel=_CHANNEL,
        session=session,
    )

    assert event is not None
    assert event.severity == "CRITICAL"
    session.post.assert_called_once()
    call = session.post.call_args
    assert call.args[0] == "http://alerting.local/api/v1/alerts/rules/recent"
    posted = call.kwargs["json"]
    assert posted["severity"] == "CRITICAL"
    assert posted["rule_id"] == "DAILY_T1_DETERMINISM_RECON"


@pytest.mark.asyncio
async def test_post_recon_alert_no_url_skips_post() -> None:
    """Empty alerting_service_url → no POST, returns None (verdict logged only)."""
    report, _ = _run_stage([_fill("t1")], [_fill("t1")])
    result = await post_recon_alert(report, alerting_service_url="", channel=_CHANNEL)
    assert result is None
