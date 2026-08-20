"""Capability-driven shard startup/continuity gate (GATE-0 M6).

``pipeline_mode_source_batch_live_replay_standardisation_2026_06_05.md`` §M6
names batch-live-reconciliation-service as **the HOME** for this gate (alongside
M4's mode-contextual precedence, shipped in :mod:`.mode_resolver`). The problem:
a shard's batch SSOT has a cutoff (e.g. yesterday's batch lands at 05:00 UTC, or
batch stops at midnight) — to operate the shard before the next batch lands, the
``[batch-cutoff -> now]`` window must be filled by SOMETHING. The fill policy is
a static per-shard fact derived from the UAC M2xM3 capability registry
(:func:`unified_api_contracts.could_exist`), NOT a feature-lookback question:

- shard has a **replay-capable** source -> autostart ``replay_<source>`` over
  ``[cutoff -> now]`` (autonomous, M7).
- no replay but a **live** source exists -> live must ALREADY be running
  (started ahead) — this gate does not itself start live.
- no replay AND no live (batch is the sole SSOT, e.g. sports fixtures) -> wait
  for batch / refuse to start / a configured-OK-gap (a per-shard DR config).

This module is the DECISION primitive only (mirrors how :mod:`.mode_resolver`
shipped standalone before a read consumer wired it in — GATE-0 Progress Log
tick 2). Actually triggering a replay run lives in market-tick-data-service;
asserting live is running / gating a live-flip lives in strategy-service — both
cross-repo consumers are tracked as the explicit remaining M6/M7 scope in the
plan above, not duplicated here.
"""

from __future__ import annotations

from enum import StrEnum

from unified_api_contracts import Mode, could_exist


class ContinuityAction(StrEnum):
    """The M6 startup/continuity action for one ``(asset_group, data_type)`` shard."""

    AUTOSTART_REPLAY = "autostart_replay"
    LIVE_MUST_BE_RUNNING = "live_must_be_running"
    WAIT_FOR_BATCH = "wait_for_batch"


class ShardUnavailableError(RuntimeError):
    """A shard cannot be operated right now and no configured gap permits it.

    Raised by :func:`assert_shard_operable` — never by
    :func:`determine_continuity_action`, which only classifies (never raises).
    """


def determine_continuity_action(asset_group: str, data_type: str) -> ContinuityAction:
    """Classify the M6 continuity action for a ``(asset_group, data_type)`` shard.

    Pure classification — never raises, never asserts anything about whether
    live is actually running or whether a gap is acceptable; that policy
    decision belongs to the caller (see :func:`assert_shard_operable`).

    Args:
        asset_group: ``cefi`` / ``defi`` / ``tradfi`` / ``prediction`` / ``sports``
            / ``reference``.
        data_type: Canonical data_type string.

    Returns:
        :class:`ContinuityAction` — ``AUTOSTART_REPLAY`` when
        ``could_exist(asset_group, data_type, Mode.REPLAY)``, else
        ``LIVE_MUST_BE_RUNNING`` when ``could_exist(..., Mode.LIVE)``, else
        ``WAIT_FOR_BATCH``.
    """
    if could_exist(asset_group, data_type, Mode.REPLAY):
        return ContinuityAction.AUTOSTART_REPLAY
    if could_exist(asset_group, data_type, Mode.LIVE):
        return ContinuityAction.LIVE_MUST_BE_RUNNING
    return ContinuityAction.WAIT_FOR_BATCH


def assert_shard_operable(
    asset_group: str,
    data_type: str,
    *,
    live_already_running: bool = False,
    gaps_ok: bool = False,
) -> ContinuityAction:
    """Resolve + enforce the M6 gate for one shard at startup.

    ``AUTOSTART_REPLAY`` always clears (the caller is expected to actually
    trigger the replay run; this gate only says it is safe to proceed).
    ``LIVE_MUST_BE_RUNNING`` clears only when the caller asserts
    ``live_already_running=True`` (an honest per-call fact, never guessed here).
    ``WAIT_FOR_BATCH`` clears only when the caller opts into ``gaps_ok=True`` —
    the per-shard "gaps are OK" DR config is explicit, never a silent default.

    Args:
        asset_group: See :func:`determine_continuity_action`.
        data_type: See :func:`determine_continuity_action`.
        live_already_running: Caller-supplied fact — is a live pipeline for this
            shard already running (started ahead of the batch cutoff)?
        gaps_ok: Caller-supplied per-shard DR config — is an unfilled
            ``[batch-cutoff -> now]`` gap acceptable for this shard?

    Returns:
        The resolved :class:`ContinuityAction` when the shard clears the gate.

    Raises:
        ShardUnavailableError: the shard cannot be operated right now (live is
            required but not confirmed running, or batch is the sole SSOT and
            no configured gap was granted).
    """
    action = determine_continuity_action(asset_group, data_type)

    if action is ContinuityAction.AUTOSTART_REPLAY:
        return action

    if action is ContinuityAction.LIVE_MUST_BE_RUNNING:
        if live_already_running:
            return action
        raise ShardUnavailableError(
            f"{asset_group}/{data_type}: no replay-capable source and live is not "
            "confirmed running — start live ahead of this shard's window before "
            "operating it (or pass live_already_running=True once it is)."
        )

    # action is WAIT_FOR_BATCH
    if gaps_ok:
        return action
    raise ShardUnavailableError(
        f"{asset_group}/{data_type}: no replay or live source available — batch "
        "is the sole SSOT for this shard and no configured-OK-gap (gaps_ok) was "
        "granted."
    )


__all__ = [
    "ContinuityAction",
    "ShardUnavailableError",
    "assert_shard_operable",
    "determine_continuity_action",
]
