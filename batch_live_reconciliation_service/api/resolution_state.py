"""Operator state attached to reconciliation breaks — pauses, exclusions, audit trail.

Implements the three W12 capabilities that were explicitly unbuilt
(`/plans/active/code_readiness_t4_execution_settlement_2026_08_19.md` § W12):

1. **Pause-before-manual-entry.** Booking a manual correction for a break means a
   human is about to move a position that automation also believes it owns. This
   module records an explicit pause on the break's ``(venue, instrument_id)``, and
   ``resolution_api.book_correction`` refuses to hand out a booking until one
   exists. The pause is the interlock, not a note — an operator cannot skip it by
   forgetting.

2. **Virtual and persistent delta exclusion.** Excluding a break's delta from
   reconciliation has two genuinely different lifetimes, and conflating them is
   how a one-off suppression silently becomes permanent:

   * :attr:`ExclusionScope.VIRTUAL` — applies to ONE run date. A later run
     re-raises the same break, which is what you want for "known timing artefact
     on today's run".
   * :attr:`ExclusionScope.PERSISTENT` — applies to every run until revoked, and
     is therefore written to GCS rather than held in memory. This is the one that
     needs an audit trail, because it suppresses a break nobody will see again.

3. **Soft-delete audit trail.** Nothing here is ever removed. Revoking a pause or
   an exclusion stamps ``deleted_at``/``deleted_by``/``delete_reason`` and leaves
   the record in place; :meth:`ResolutionStateStore.all_pauses` and
   :meth:`ResolutionStateStore.all_exclusions` return active and revoked records
   alike. An FCA-relevant surface must be able to answer "who
   suppressed this break, when, and who un-suppressed it" after the fact —
   a hard delete cannot answer that.

Storage: VIRTUAL exclusions and pauses are process-local (matching the existing
``_resolutions`` precedent in ``resolution_api``); PERSISTENT exclusions are
read/written through the same ``get_storage_client()`` + ``recon_bucket`` path the
Stage-5 summaries use, so they genuinely survive a restart. That asymmetry is
deliberate and is the definition of the two scopes, not an oversight.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from unified_trading_library import get_storage_client, log_event

logger = logging.getLogger(__name__)

#: GCS object holding persistent exclusions, alongside the Stage-5 summaries.
PERSISTENT_EXCLUSIONS_BLOB = "t1-recon/recon/exclusions.json"


class ExclusionScope(StrEnum):
    """How long a delta exclusion lasts. See the module docstring."""

    VIRTUAL = "virtual"
    """One run date only — a later run re-raises the break."""

    PERSISTENT = "persistent"
    """Every run until revoked. Written to GCS; survives restart."""


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class TradingPause:
    """A recorded pause on one ``(venue, instrument_id)``.

    Soft-deleted on revoke — see :attr:`revoked_at`.
    """

    venue: str
    instrument_id: str
    break_id: str
    reason: str
    paused_by: str
    paused_at: datetime
    revoked_at: datetime | None = None
    revoked_by: str | None = None
    revoke_reason: str | None = None

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None

    @property
    def key(self) -> tuple[str, str]:
        return (self.venue.upper(), self.instrument_id.upper())


@dataclass(frozen=True)
class DeltaExclusion:
    """A break excluded from reconciliation, with its scope and audit fields."""

    break_id: str
    scope: ExclusionScope
    reason: str
    excluded_by: str
    excluded_at: datetime
    #: The run date a VIRTUAL exclusion applies to. Always ``None`` for
    #: PERSISTENT — a persistent exclusion is not tied to one run.
    run_date: str | None = None
    revoked_at: datetime | None = None
    revoked_by: str | None = None
    revoke_reason: str | None = None

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None

    def applies_to(self, run_date: str) -> bool:
        """Whether this exclusion suppresses its break for ``run_date``."""
        if not self.is_active:
            return False
        if self.scope is ExclusionScope.PERSISTENT:
            return True
        return self.run_date == run_date

    def to_json(self) -> dict[str, object]:
        return {
            "break_id": self.break_id,
            "scope": self.scope.value,
            "reason": self.reason,
            "excluded_by": self.excluded_by,
            "excluded_at": self.excluded_at.isoformat(),
            "run_date": self.run_date,
            "revoked_at": self.revoked_at.isoformat() if self.revoked_at else None,
            "revoked_by": self.revoked_by,
            "revoke_reason": self.revoke_reason,
        }

    @staticmethod
    def from_json(raw: dict[str, object]) -> DeltaExclusion:
        revoked_at_raw = raw.get("revoked_at")
        return DeltaExclusion(
            break_id=str(raw["break_id"]),
            scope=ExclusionScope(str(raw["scope"])),
            reason=str(raw["reason"]),
            excluded_by=str(raw["excluded_by"]),
            excluded_at=datetime.fromisoformat(str(raw["excluded_at"])),
            run_date=cast("str | None", raw.get("run_date")),
            revoked_at=datetime.fromisoformat(str(revoked_at_raw)) if revoked_at_raw else None,
            revoked_by=cast("str | None", raw.get("revoked_by")),
            revoke_reason=cast("str | None", raw.get("revoke_reason")),
        )


class ExclusionPersistenceError(RuntimeError):
    """A PERSISTENT exclusion could not be written to GCS.

    Raised rather than swallowed on purpose. A persistent exclusion accepted but
    not stored is the worst outcome available: the operator believes a break is
    suppressed for every future run, and it is not. The recon bucket has a real
    history of not existing
    (`/plans/active/issues/recon_bucket_missing_nightly_recon_failing_2026_07_13.md`
    — the nightly job failed 55/56 runs against a bucket that never existed), so
    this is a reachable condition, not a defensive hypothetical.
    """

    def __init__(self, bucket: str, cause: Exception) -> None:
        super().__init__(
            f"Persistent exclusion NOT saved: writing {PERSISTENT_EXCLUSIONS_BLOB!r} to bucket {bucket!r} "
            f"failed ({type(cause).__name__}: {cause}). The exclusion is NOT in effect — the break will keep "
            "being raised. Retry once the bucket is reachable, or use a virtual exclusion for this run."
        )
        self.bucket = bucket


class PauseRequiredError(RuntimeError):
    """Raised when a manual correction is requested without an active pause.

    The interlock behind W12's pause-before-manual-entry: booking a correction
    moves a position automation also believes it owns, so the pause must exist
    BEFORE the booking is handed out, not as a note afterwards.
    """

    def __init__(self, venue: str, instrument_id: str) -> None:
        super().__init__(
            f"No active trading pause for venue={venue!r} instrument_id={instrument_id!r}. "
            "Pause before booking a manual correction — a correction booked while automation "
            "is still trading the same position can double-apply."
        )
        self.venue = venue
        self.instrument_id = instrument_id


@dataclass
class ResolutionStateStore:  # CORRECT-LOCAL: service-internal operator state, not a cross-service contract
    """Append-only store for pauses and exclusions. Never hard-deletes.

    Thread safety: NOT thread-safe; single-process FastAPI use, matching the
    existing ``_resolutions`` dict this sits beside.
    """

    _pauses: list[TradingPause] = field(default_factory=list)
    _virtual_exclusions: list[DeltaExclusion] = field(default_factory=list)
    _persistent_cache: list[DeltaExclusion] | None = field(default=None)

    # ---------------------------------------------------------------- pauses

    def pause(self, *, venue: str, instrument_id: str, break_id: str, reason: str, paused_by: str) -> TradingPause:
        """Record a pause. Re-pausing an already-paused key appends a new record
        rather than mutating the old one, so the audit trail keeps both."""
        entry = TradingPause(
            venue=venue,
            instrument_id=instrument_id,
            break_id=break_id,
            reason=reason,
            paused_by=paused_by,
            paused_at=_now(),
        )
        self._pauses.append(entry)
        log_event(
            "RECONCILIATION_TRADING_PAUSED",
            details={
                "venue": venue,
                "instrument_id": instrument_id,
                "break_id": break_id,
                "paused_by": paused_by,
                "reason": reason,
            },
        )
        return entry

    def active_pause(self, venue: str, instrument_id: str) -> TradingPause | None:
        """The most recent active pause for this key, or ``None``."""
        key = (venue.upper(), instrument_id.upper())
        for entry in reversed(self._pauses):
            if entry.key == key and entry.is_active:
                return entry
        return None

    def require_pause(self, venue: str, instrument_id: str) -> TradingPause:
        """Return the active pause or raise :class:`PauseRequiredError`."""
        entry = self.active_pause(venue, instrument_id)
        if entry is None:
            raise PauseRequiredError(venue, instrument_id)
        return entry

    def revoke_pause(self, *, venue: str, instrument_id: str, revoked_by: str, reason: str) -> TradingPause | None:
        """Soft-delete the active pause for this key. Returns the revoked record."""
        key = (venue.upper(), instrument_id.upper())
        for index in range(len(self._pauses) - 1, -1, -1):
            entry = self._pauses[index]
            if entry.key == key and entry.is_active:
                revoked = replace(entry, revoked_at=_now(), revoked_by=revoked_by, revoke_reason=reason)
                self._pauses[index] = revoked
                log_event(
                    "RECONCILIATION_TRADING_PAUSE_REVOKED",
                    details={
                        "venue": venue,
                        "instrument_id": instrument_id,
                        "revoked_by": revoked_by,
                        "reason": reason,
                    },
                )
                return revoked
        return None

    def all_pauses(self) -> tuple[TradingPause, ...]:
        """Every pause ever recorded, active and revoked — the audit view."""
        return tuple(self._pauses)

    # ------------------------------------------------------------ exclusions

    def exclude(
        self,
        *,
        break_id: str,
        scope: ExclusionScope,
        reason: str,
        excluded_by: str,
        run_date: str | None,
        bucket: str,
    ) -> DeltaExclusion:
        """Exclude a break's delta.

        A VIRTUAL exclusion requires ``run_date`` — without it there is no run for
        it to apply to, and silently promoting it to persistent is exactly the
        failure this split exists to prevent.
        """
        if scope is ExclusionScope.VIRTUAL and not run_date:
            raise ValueError("A VIRTUAL exclusion requires run_date — it applies to exactly one run.")
        entry = DeltaExclusion(
            break_id=break_id,
            scope=scope,
            reason=reason,
            excluded_by=excluded_by,
            excluded_at=_now(),
            run_date=run_date if scope is ExclusionScope.VIRTUAL else None,
        )
        if scope is ExclusionScope.PERSISTENT:
            records = [*self._load_persistent(bucket), entry]
            self._write_persistent(bucket, records)
        else:
            self._virtual_exclusions.append(entry)
        log_event(
            "RECONCILIATION_DELTA_EXCLUDED",
            details={
                "break_id": break_id,
                "scope": scope.value,
                "excluded_by": excluded_by,
                "reason": reason,
                "run_date": run_date or "",
            },
        )
        return entry

    def revoke_exclusion(self, *, break_id: str, revoked_by: str, reason: str, bucket: str) -> DeltaExclusion | None:
        """Soft-delete the active exclusion for ``break_id`` in either scope."""
        for index in range(len(self._virtual_exclusions) - 1, -1, -1):
            entry = self._virtual_exclusions[index]
            if entry.break_id == break_id and entry.is_active:
                revoked = replace(entry, revoked_at=_now(), revoked_by=revoked_by, revoke_reason=reason)
                self._virtual_exclusions[index] = revoked
                self._log_revoke(revoked)
                return revoked

        records = self._load_persistent(bucket)
        for index in range(len(records) - 1, -1, -1):
            entry = records[index]
            if entry.break_id == break_id and entry.is_active:
                revoked = replace(entry, revoked_at=_now(), revoked_by=revoked_by, revoke_reason=reason)
                records[index] = revoked
                self._write_persistent(bucket, records)
                self._log_revoke(revoked)
                return revoked
        return None

    @staticmethod
    def _log_revoke(entry: DeltaExclusion) -> None:
        log_event(
            "RECONCILIATION_DELTA_EXCLUSION_REVOKED",
            details={
                "break_id": entry.break_id,
                "scope": entry.scope.value,
                "revoked_by": entry.revoked_by or "",
                "reason": entry.revoke_reason or "",
            },
        )

    def excluded_break_ids(self, *, run_date: str, bucket: str) -> frozenset[str]:
        """Break IDs suppressed for ``run_date`` across both scopes."""
        active = [e for e in (*self._virtual_exclusions, *self._load_persistent(bucket)) if e.applies_to(run_date)]
        return frozenset(e.break_id for e in active)

    def all_exclusions(self, bucket: str) -> tuple[DeltaExclusion, ...]:
        """Every exclusion ever recorded, active and revoked — the audit view."""
        return (*self._virtual_exclusions, *self._load_persistent(bucket))

    # --------------------------------------------------------- GCS persistence

    def _load_persistent(self, bucket: str) -> list[DeltaExclusion]:
        """Read persistent exclusions from GCS, caching for this process.

        A missing object is an empty list — no exclusions yet. A CORRUPT object is
        NOT: it is logged and treated as empty for reads, because silently
        suppressing breaks based on half-parsed state is worse than re-raising
        them, and the write path rewrites the whole object anyway.
        """
        if self._persistent_cache is not None:
            return self._persistent_cache
        client = get_storage_client()
        try:
            raw = client.download_bytes(bucket=bucket, blob_path=PERSISTENT_EXCLUSIONS_BLOB)
        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError):
            self._persistent_cache = []
            return self._persistent_cache
        try:
            decoded = cast(list[dict[str, object]], json.loads(raw.decode("utf-8")))
            self._persistent_cache = [DeltaExclusion.from_json(item) for item in decoded]
        except (ValueError, TypeError, KeyError) as exc:
            logger.error(
                "Persistent exclusions at %s are unreadable (%s) — treating as EMPTY so breaks are re-raised "
                "rather than suppressed by half-parsed state.",
                PERSISTENT_EXCLUSIONS_BLOB,
                exc,
            )
            self._persistent_cache = []
        return self._persistent_cache

    def _write_persistent(self, bucket: str, records: list[DeltaExclusion]) -> None:
        """Rewrite the whole persistent-exclusion object, revoked records included.

        The cache is updated only AFTER a successful write, so a failed write
        cannot leave this process believing an exclusion is in effect.
        """
        client = get_storage_client()
        payload = json.dumps([r.to_json() for r in records], indent=2).encode("utf-8")
        try:
            _ = client.upload_bytes(bucket=bucket, blob_path=PERSISTENT_EXCLUSIONS_BLOB, data=payload)
        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError) as exc:
            logger.error("Persistent exclusion write to %s failed: %s", bucket, exc)
            raise ExclusionPersistenceError(bucket, exc) from exc
        self._persistent_cache = records

    def invalidate_cache(self) -> None:
        """Drop the persistent-exclusion cache so the next read re-fetches."""
        self._persistent_cache = None


__all__ = [
    "PERSISTENT_EXCLUSIONS_BLOB",
    "DeltaExclusion",
    "ExclusionPersistenceError",
    "ExclusionScope",
    "PauseRequiredError",
    "ResolutionStateStore",
    "TradingPause",
]
