"""Edge-case tests for UTL reconcile_shard() — empty, single-row, schema-drift, large.

Tests the pure function directly (no GCS, no service-layer wiring) to pin
verdicts for corner inputs that the nominal test suite skips.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from unified_trading_library import BatchLiveReconciliationReport, reconcile_shard

pytestmark = pytest.mark.unit

_ASSET_GROUP = "cefi"
_VENUE = "BINANCE-SPOT"
_DATA_TYPE = "trades"
_INSTRUMENT_ID = "BTC-USDT"
_DAY = "2026-05-18"

_RowDict = dict[str, object]
_RowComparator = Callable[[_RowDict, _RowDict], bool]


def _call(
    batch_rows: list[_RowDict],
    live_rows: list[_RowDict],
    row_comparator: _RowComparator | None = None,
) -> BatchLiveReconciliationReport:
    if row_comparator is not None:
        return reconcile_shard(
            asset_group=_ASSET_GROUP,
            venue=_VENUE,
            data_type=_DATA_TYPE,
            instrument_id=_INSTRUMENT_ID,
            day=_DAY,
            batch_rows=batch_rows,
            live_rows=live_rows,
            row_comparator=row_comparator,
        )
    return reconcile_shard(
        asset_group=_ASSET_GROUP,
        venue=_VENUE,
        data_type=_DATA_TYPE,
        instrument_id=_INSTRUMENT_ID,
        day=_DAY,
        batch_rows=batch_rows,
        live_rows=live_rows,
    )


class TestEmptyShardEdgeCase:
    """Both sides empty → MATCH (zero-row shard is a legitimate equality)."""

    def test_both_empty_is_match(self) -> None:
        report = _call(batch_rows=[], live_rows=[])
        assert report.verdict == "MATCH"

    def test_both_empty_row_counts_zero(self) -> None:
        report = _call(batch_rows=[], live_rows=[])
        assert report.batch_row_count == 0
        assert report.live_row_count == 0

    def test_both_empty_rows_compared_zero(self) -> None:
        report = _call(batch_rows=[], live_rows=[])
        assert report.rows_compared == 0

    def test_batch_empty_live_nonempty_is_row_count_mismatch(self) -> None:
        live: list[_RowDict] = [{"price": 1.0}]
        report = _call(batch_rows=[], live_rows=live)
        assert report.verdict == "ROW_COUNT_MISMATCH"

    def test_batch_nonempty_live_empty_is_row_count_mismatch(self) -> None:
        batch: list[_RowDict] = [{"price": 1.0}]
        report = _call(batch_rows=batch, live_rows=[])
        assert report.verdict == "ROW_COUNT_MISMATCH"


class TestSingleRowEdgeCase:
    """Single-row shards — most degenerate non-empty case."""

    def test_single_identical_row_is_match(self) -> None:
        row: _RowDict = {"ts": 1000, "price": 42.0, "qty": 1.5}
        report = _call(batch_rows=[row], live_rows=[row])
        assert report.verdict == "MATCH"
        assert report.rows_compared == 1
        assert report.rows_within_tolerance == 1
        assert report.rows_outside_tolerance == 0

    def test_single_row_close_within_tolerance(self) -> None:
        batch: list[_RowDict] = [{"close": 100.0}]
        live: list[_RowDict] = [{"close": 100.005}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.verdict == "MATCH"

    def test_single_row_outside_tolerance_is_value_mismatch(self) -> None:
        batch: list[_RowDict] = [{"close": 100.0}]
        live: list[_RowDict] = [{"close": 105.0}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.verdict == "VALUE_MISMATCH"
        assert report.rows_outside_tolerance == 1

    def test_single_row_preserves_metadata(self) -> None:
        row: _RowDict = {"close": 1.0}
        report = _call(batch_rows=[row], live_rows=[row])
        assert report.asset_group == "cefi"
        assert report.venue == "BINANCE-SPOT"
        assert report.instrument_id == "BTC-USDT"
        assert report.day == "2026-05-18"


class TestSchemaDriftEdgeCase:
    """Schema mismatch (different column sets) → SCHEMA_MISMATCH."""

    def test_different_keys_is_schema_mismatch(self) -> None:
        batch: list[_RowDict] = [{"price": 1.0, "qty": 2.0}]
        live: list[_RowDict] = [{"price": 1.0, "volume": 2.0}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.verdict == "SCHEMA_MISMATCH"
        assert not report.schema_match

    def test_schema_mismatch_rows_compared_zero(self) -> None:
        batch: list[_RowDict] = [{"a": 1}]
        live: list[_RowDict] = [{"b": 1}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.rows_compared == 0

    def test_extra_column_in_batch_is_schema_mismatch(self) -> None:
        batch: list[_RowDict] = [{"price": 1.0, "extra_field": "X"}]
        live: list[_RowDict] = [{"price": 1.0}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.verdict == "SCHEMA_MISMATCH"

    def test_extra_column_in_live_is_schema_mismatch(self) -> None:
        batch: list[_RowDict] = [{"price": 1.0}]
        live: list[_RowDict] = [{"price": 1.0, "live_only_field": True}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.verdict == "SCHEMA_MISMATCH"

    def test_same_keys_different_value_types_passes_schema_check(self) -> None:
        batch: list[_RowDict] = [{"price": 1.0}]
        live: list[_RowDict] = [{"price": "1.0"}]
        report = _call(batch_rows=batch, live_rows=live)
        assert report.schema_match


class TestLargeShardEdgeCase:
    """Large row counts (memory-pressure proxy) — 10 000 rows must complete efficiently."""

    def test_10k_rows_identical_is_match(self) -> None:
        rows: list[_RowDict] = [{"close": float(i), "ts": i} for i in range(10_000)]
        report = _call(batch_rows=rows, live_rows=rows)
        assert report.verdict == "MATCH"
        assert report.rows_compared == 10_000
        assert report.rows_within_tolerance == 10_000
        assert report.rows_outside_tolerance == 0

    def test_10k_rows_one_mismatch_is_value_mismatch(self) -> None:
        rows: list[_RowDict] = [{"close": float(i)} for i in range(10_000)]
        live: list[_RowDict] = list(rows)
        live[5000] = {"close": 999_999.0}
        report = _call(batch_rows=rows, live_rows=live)
        assert report.verdict == "VALUE_MISMATCH"
        assert report.rows_outside_tolerance == 1
        assert report.rows_within_tolerance == 9_999

    def test_10k_rows_count_mismatch_short_circuits(self) -> None:
        rows: list[_RowDict] = [{"close": float(i)} for i in range(10_000)]
        report = _call(batch_rows=rows, live_rows=rows[:-1])
        assert report.verdict == "ROW_COUNT_MISMATCH"
        assert report.rows_compared == 0

    def test_custom_comparator_applied_across_large_shard(self) -> None:
        rows: list[_RowDict] = [{"value": float(i)} for i in range(1_000)]

        def always_match(_batch_row: _RowDict, _live: _RowDict) -> bool:
            return True

        report = _call(batch_rows=rows, live_rows=rows, row_comparator=always_match)
        assert report.verdict == "MATCH"
        assert report.rows_within_tolerance == 1_000
