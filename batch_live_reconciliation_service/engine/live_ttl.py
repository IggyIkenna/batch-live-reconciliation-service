"""Batch/live reconciliation tolerance + live-cell TTL eligibility (GATE-0 T+1 TTL).

``pipeline_mode_source_batch_live_replay_standardisation_2026_06_05.md`` §"T+1
batch/live reconciliation + `live` TTL" names batch-live-reconciliation-service
as the home for this tranche: "the batch-live-reconciliation-service confirms
batch approx-equals live within a tolerance, then a TTL clears the now-redundant
`live` cells (long-lived `replay` stays where batch never existed)."

This module ships the DECISION half of that tranche: given a cell's batch vs
live values plus the two config knobs (a reconciliation tolerance and a TTL
horizon), decide whether the cell's `live` row is (a) reconciled against batch
and (b) old enough past that reconciliation to be TTL-eligible.

It is deliberately a PURE, read-only decision layer — it never mutates the
manifest. Applying the clear (deleting/superseding the redundant `live`
:class:`~unified_trading_library.manifest_writer.AvailabilityRecord`) needs a
write-path helper in unified-trading-library that does not exist yet (the
plan's own "+ UTL TTL helper" note); until that lands, callers should treat
:func:`ttl_eligible_cells` as a dry-run report, not an executable clear.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class LiveTtlConfig:
    """Config knobs for the batch/live TTL clearing decision.

    Attributes:
        reconciliation_tolerance: maximum allowed ``|batch_value - live_value|``
            for the two sides to be considered reconciled ("batch approx-equals
            live"). Units are whatever the caller's metric uses (row count,
            notional, etc) — this module has no opinion on the metric itself.
            Defaults to ``0.0`` (exact match required) so a caller must
            deliberately widen it for a metric where live and batch are
            expected to diverge slightly (unlike the paper<->batch determinism
            proof elsewhere in this plan, live carries real venue fills, so
            batch<->live is a TOLERANCE check, not an epsilon=0 proof).
        ttl_horizon_days: once a live cell has been reconciled (batch caught up
            to it) for at least this many days, it becomes ELIGIBLE for TTL
            clearing. A short grace window avoids clearing a cell the moment
            batch lands. Defaults to 30 days.
    """

    reconciliation_tolerance: float = 0.0
    ttl_horizon_days: int = 30


@dataclass(frozen=True)
class LiveCellReconciliationResult:
    """One shard/date cell's batch-vs-live reconciliation inputs.

    ``written_at`` is the manifest ``AvailabilityRecord.written_at`` ISO
    timestamp for the `live` row — the age clock for TTL eligibility runs from
    when the row was written, not from ``date`` (a late-consolidated cell should
    not age out early).
    """

    date: str
    asset_group: str
    data_type: str
    batch_value: float
    live_value: float
    written_at: str

    @property
    def delta(self) -> float:
        """Absolute batch-vs-live value delta."""
        return abs(self.batch_value - self.live_value)


def is_reconciled(cell: LiveCellReconciliationResult, config: LiveTtlConfig) -> bool:
    """True iff ``cell``'s batch and live values agree within the configured tolerance."""
    return cell.delta <= config.reconciliation_tolerance


def _parse_written_at(value: str) -> datetime | None:
    """Parse an ISO timestamp string, defaulting a naive result to UTC.

    Returns ``None`` on a blank or unparseable value — TTL eligibility fails
    CLOSED on an unparseable timestamp (never guesses an age).
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def is_ttl_eligible(
    cell: LiveCellReconciliationResult,
    config: LiveTtlConfig,
    *,
    now: datetime | None = None,
) -> bool:
    """True iff ``cell``'s `live` row is a candidate for TTL clearing.

    Requires BOTH: (a) :func:`is_reconciled` — a `live` row that batch has not
    yet confirmed must never be cleared, it may be the only copy; a long-lived
    `replay` cell (where batch never existed for that window) is likewise never
    a candidate, since it was never compared here in the first place — and (b)
    the row has been written at least ``config.ttl_horizon_days`` ago.

    Pure decision only — see the module docstring for why this never mutates
    the manifest.
    """
    if not is_reconciled(cell, config):
        return False
    written_at = _parse_written_at(cell.written_at)
    if written_at is None:
        return False
    effective_now = now if now is not None else datetime.now(UTC)
    age = effective_now - written_at
    return age >= timedelta(days=config.ttl_horizon_days)


def ttl_eligible_cells(
    cells: list[LiveCellReconciliationResult],
    config: LiveTtlConfig,
    *,
    now: datetime | None = None,
) -> list[LiveCellReconciliationResult]:
    """Return the subset of ``cells`` whose `live` row is TTL-eligible.

    A read-only report (see module docstring) — the caller is responsible for
    actually clearing the returned cells once a UTL write helper exists.
    """
    return [cell for cell in cells if is_ttl_eligible(cell, config, now=now)]


__all__ = [
    "LiveCellReconciliationResult",
    "LiveTtlConfig",
    "is_reconciled",
    "is_ttl_eligible",
    "ttl_eligible_cells",
]
