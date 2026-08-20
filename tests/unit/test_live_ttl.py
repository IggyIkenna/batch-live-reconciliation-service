"""Unit tests for the batch/live reconciliation-tolerance + TTL-eligibility decision layer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from batch_live_reconciliation_service.engine.live_ttl import (
    LiveCellReconciliationResult,
    LiveTtlConfig,
    is_reconciled,
    is_ttl_eligible,
    ttl_eligible_cells,
)

_NOW = datetime(2026, 8, 20, tzinfo=UTC)


def _cell(*, batch: float, live: float, written_at: str) -> LiveCellReconciliationResult:
    return LiveCellReconciliationResult(
        date="2026-07-01",
        asset_group="cefi",
        data_type="trades",
        batch_value=batch,
        live_value=live,
        written_at=written_at,
    )


def test_is_reconciled_exact_match_within_default_zero_tolerance() -> None:
    cell = _cell(batch=100.0, live=100.0, written_at=(_NOW - timedelta(days=60)).isoformat())
    assert is_reconciled(cell, LiveTtlConfig()) is True


def test_is_reconciled_false_when_delta_exceeds_default_zero_tolerance() -> None:
    cell = _cell(batch=100.0, live=101.0, written_at=(_NOW - timedelta(days=60)).isoformat())
    assert is_reconciled(cell, LiveTtlConfig()) is False


def test_is_reconciled_true_within_widened_tolerance() -> None:
    cell = _cell(batch=100.0, live=101.0, written_at=(_NOW - timedelta(days=60)).isoformat())
    assert is_reconciled(cell, LiveTtlConfig(reconciliation_tolerance=2.0)) is True


def test_is_ttl_eligible_true_when_reconciled_and_past_horizon() -> None:
    cell = _cell(batch=10.0, live=10.0, written_at=(_NOW - timedelta(days=31)).isoformat())
    config = LiveTtlConfig(ttl_horizon_days=30)
    assert is_ttl_eligible(cell, config, now=_NOW) is True


def test_is_ttl_eligible_false_when_within_grace_window() -> None:
    cell = _cell(batch=10.0, live=10.0, written_at=(_NOW - timedelta(days=5)).isoformat())
    config = LiveTtlConfig(ttl_horizon_days=30)
    assert is_ttl_eligible(cell, config, now=_NOW) is False


def test_is_ttl_eligible_false_when_not_reconciled_regardless_of_age() -> None:
    """A live row batch has never confirmed must never be treated as TTL-eligible,
    no matter how old — it may be the only copy of that data."""
    cell = _cell(batch=10.0, live=999.0, written_at=(_NOW - timedelta(days=365)).isoformat())
    config = LiveTtlConfig(ttl_horizon_days=30)
    assert is_ttl_eligible(cell, config, now=_NOW) is False


def test_is_ttl_eligible_fails_closed_on_blank_written_at() -> None:
    cell = _cell(batch=10.0, live=10.0, written_at="")
    assert is_ttl_eligible(cell, LiveTtlConfig(), now=_NOW) is False


def test_is_ttl_eligible_fails_closed_on_unparseable_written_at() -> None:
    cell = _cell(batch=10.0, live=10.0, written_at="not-a-timestamp")
    assert is_ttl_eligible(cell, LiveTtlConfig(), now=_NOW) is False


def test_is_ttl_eligible_handles_naive_written_at_as_utc() -> None:
    naive = (_NOW - timedelta(days=45)).replace(tzinfo=None).isoformat()
    cell = _cell(batch=10.0, live=10.0, written_at=naive)
    assert is_ttl_eligible(cell, LiveTtlConfig(ttl_horizon_days=30), now=_NOW) is True


def test_ttl_eligible_cells_filters_the_mixed_set() -> None:
    reconciled_old = _cell(batch=5.0, live=5.0, written_at=(_NOW - timedelta(days=40)).isoformat())
    reconciled_young = _cell(batch=5.0, live=5.0, written_at=(_NOW - timedelta(days=1)).isoformat())
    unreconciled_old = _cell(batch=5.0, live=50.0, written_at=(_NOW - timedelta(days=40)).isoformat())

    got = ttl_eligible_cells(
        [reconciled_old, reconciled_young, unreconciled_old],
        LiveTtlConfig(ttl_horizon_days=30),
        now=_NOW,
    )

    assert got == [reconciled_old]
