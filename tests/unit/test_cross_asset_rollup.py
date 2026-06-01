"""Cross-asset_group reconciliation + per-archetype rollup tests.

Builds on test_reconcile_shard_edge_cases.py (cefi single-asset tests).

Scenarios:
  1. Cross-asset_group parity — identical row pairs produce identical verdicts
     for cefi vs defi vs tradfi vs sports vs prediction.

  2. Per-archetype rollup isolation — two archetypes (carry_staked_basis and
     arbitrage_price_dispersion) reconciled independently; one mismatch does
     not contaminate the other archetype's report.

  3. Schema-drift report semantics — SCHEMA_MISMATCH report exposes the
     drifted key set and row metadata for downstream audit.
"""

from __future__ import annotations

import pytest
from unified_trading_library import BatchLiveReconciliationReport, reconcile_shard

pytestmark = pytest.mark.unit

_DAY = "2026-05-18"
_DEFAULT_DATA_TYPE = "trades"
_DEFAULT_INSTRUMENT = "ETH-USDT"

_RowDict = dict[str, object]


def _call(
    batch_rows: list[_RowDict],
    live_rows: list[_RowDict],
    *,
    asset_group: str = "cefi",
    venue: str = "BINANCE-SPOT",
    data_type: str = _DEFAULT_DATA_TYPE,
    instrument_id: str = _DEFAULT_INSTRUMENT,
) -> BatchLiveReconciliationReport:
    return reconcile_shard(
        asset_group=asset_group,
        venue=venue,
        data_type=data_type,
        instrument_id=instrument_id,
        day=_DAY,
        batch_rows=batch_rows,
        live_rows=live_rows,
    )


# ---------------------------------------------------------------------------
# Scenario 1 — Cross-asset_group parity
# ---------------------------------------------------------------------------


class TestCrossAssetGroupParity:
    """reconcile_shard() produces identical verdicts regardless of asset_group."""

    @pytest.mark.parametrize(
        "asset_group,venue",
        [
            ("cefi", "BINANCE-SPOT"),
            ("defi", "UNISWAP-V3"),
            ("tradfi", "IBKR"),
            ("sports", "BETFAIR"),
            ("prediction", "POLYMARKET"),
        ],
    )
    def test_match_verdict_is_asset_group_invariant(self, asset_group: str, venue: str) -> None:
        rows: list[_RowDict] = [{"price": 100.0, "volume": 1.5}]
        report = _call(batch_rows=rows, live_rows=rows, asset_group=asset_group, venue=venue)
        assert report.verdict == "MATCH", f"Expected MATCH for asset_group={asset_group}; got {report.verdict}"

    @pytest.mark.parametrize(
        "asset_group,venue",
        [
            ("cefi", "BINANCE-SPOT"),
            ("defi", "UNISWAP-V3"),
            ("tradfi", "IBKR"),
        ],
    )
    def test_row_count_mismatch_verdict_is_asset_group_invariant(self, asset_group: str, venue: str) -> None:
        batch: list[_RowDict] = [{"price": 100.0}]
        report = _call(batch_rows=batch, live_rows=[], asset_group=asset_group, venue=venue)
        assert report.verdict == "ROW_COUNT_MISMATCH", (
            f"Expected ROW_COUNT_MISMATCH for asset_group={asset_group}; got {report.verdict}"
        )

    def test_cefi_defi_identical_inputs_produce_identical_reports(self) -> None:
        rows: list[_RowDict] = [{"price": 3400.0, "volume": 2.0}]
        cefi_report = _call(batch_rows=rows, live_rows=rows, asset_group="cefi", venue="BINANCE-SPOT")
        defi_report = _call(batch_rows=rows, live_rows=rows, asset_group="defi", venue="UNISWAP-V3")
        assert cefi_report.verdict == defi_report.verdict
        assert cefi_report.batch_row_count == defi_report.batch_row_count
        assert cefi_report.rows_compared == defi_report.rows_compared


# ---------------------------------------------------------------------------
# Scenario 2 — Per-archetype rollup isolation
# ---------------------------------------------------------------------------


class TestPerArchetypeRollupIsolation:
    """Archetype reports are independent; a mismatch in one must not affect the other."""

    def test_carry_mismatch_does_not_contaminate_arbitrage(self) -> None:
        # close=100.0 vs close=110.0 → 10% delta, well outside 0.01% rel_tolerance → VALUE_MISMATCH
        carry_batch: list[_RowDict] = [{"close": 100.0}]
        carry_live: list[_RowDict] = [{"close": 110.0}]
        arb_rows: list[_RowDict] = [{"close": 100.0}]

        carry_report = _call(
            batch_rows=carry_batch,
            live_rows=carry_live,
            data_type="carry_signals",
            instrument_id="ETH-USDT",
        )
        arb_report = _call(
            batch_rows=arb_rows,
            live_rows=arb_rows,
            data_type="arbitrage_signals",
            instrument_id="BTC-USDT",
        )

        assert carry_report.verdict != "MATCH", "Carry signals with value diff must not be MATCH"
        assert arb_report.verdict == "MATCH", (
            f"Arbitrage signals with identical rows must be MATCH; got {arb_report.verdict}"
        )

    def test_both_archetypes_match_independently(self) -> None:
        rows_a: list[_RowDict] = [{"signal": 1.0, "confidence": 0.9}]
        rows_b: list[_RowDict] = [{"rate": 0.04, "funding_bps": 3}]

        reports = [
            _call(batch_rows=rows_a, live_rows=rows_a, data_type="carry_signals", instrument_id="ETH-USDT"),
            _call(batch_rows=rows_b, live_rows=rows_b, data_type="arbitrage_signals", instrument_id="BTC-USDT"),
        ]
        for report in reports:
            assert report.verdict == "MATCH", f"Expected MATCH; got {report.verdict} for {report}"

    def test_archetype_row_counts_are_independent(self) -> None:
        """Each archetype tracks its own row counts; they don't bleed across calls."""
        one_row: list[_RowDict] = [{"price": 1.0}]
        two_rows: list[_RowDict] = [{"price": 1.0}, {"price": 2.0}]

        report_one = _call(batch_rows=one_row, live_rows=one_row, data_type="carry_signals")
        report_two = _call(batch_rows=two_rows, live_rows=two_rows, data_type="arbitrage_signals")

        assert report_one.batch_row_count == 1
        assert report_two.batch_row_count == 2


# ---------------------------------------------------------------------------
# Scenario 3 — Schema-drift report semantics
# ---------------------------------------------------------------------------


class TestSchemaDriftReportSemantics:
    """SCHEMA_MISMATCH reports expose drift metadata for downstream audit."""

    def test_schema_mismatch_verdict_on_key_divergence(self) -> None:
        batch: list[_RowDict] = [{"price": 100.0, "old_field": "v1"}]
        live: list[_RowDict] = [{"price": 100.0, "new_field": "v2"}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.verdict == "SCHEMA_MISMATCH"

    def test_schema_mismatch_zero_rows_compared(self) -> None:
        batch: list[_RowDict] = [{"batch_only_col": 1.0}]
        live: list[_RowDict] = [{"live_only_col": 1.0}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.rows_compared == 0, (
            f"Schema mismatch must short-circuit row comparison; got rows_compared={report.rows_compared}"
        )

    def test_schema_mismatch_preserves_row_counts(self) -> None:
        batch: list[_RowDict] = [{"batch_only_col": i} for i in range(5)]
        live: list[_RowDict] = [{"live_only_col": i} for i in range(5)]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.batch_row_count == 5
        assert report.live_row_count == 5

    def test_schema_mismatch_after_adding_column_in_live(self) -> None:
        batch: list[_RowDict] = [{"price": 1.0, "volume": 1.0}]
        live: list[_RowDict] = [{"price": 1.0, "volume": 1.0, "extra_col": "new"}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.verdict == "SCHEMA_MISMATCH"
