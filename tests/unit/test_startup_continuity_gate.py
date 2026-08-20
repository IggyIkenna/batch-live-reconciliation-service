"""Unit tests for the M6 capability-driven shard startup/continuity gate."""

from __future__ import annotations

import pytest
from unified_api_contracts import Mode

from batch_live_reconciliation_service.engine import startup_continuity_gate as gate


def test_determine_continuity_action_real_replay_capable_shard() -> None:
    """Sanity check against the REAL UAC registry (not mocked): a chain-RPC-backed
    DeFi shard is replay-capable end to end, so the gate must autostart replay."""
    assert gate.determine_continuity_action("defi", "dex_pool_state") == gate.ContinuityAction.AUTOSTART_REPLAY


def test_determine_continuity_action_replay_capable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.REPLAY)
    assert gate.determine_continuity_action("cefi", "trades") == gate.ContinuityAction.AUTOSTART_REPLAY


def test_determine_continuity_action_live_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.LIVE)
    assert gate.determine_continuity_action("cefi", "l2_book") == gate.ContinuityAction.LIVE_MUST_BE_RUNNING


def test_determine_continuity_action_batch_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.BATCH)
    assert gate.determine_continuity_action("sports", "fixtures") == gate.ContinuityAction.WAIT_FOR_BATCH


def test_determine_continuity_action_nothing_capable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: False)
    assert gate.determine_continuity_action("unregistered", "unknown") == gate.ContinuityAction.WAIT_FOR_BATCH


def test_assert_shard_operable_replay_always_clears(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.REPLAY)
    assert gate.assert_shard_operable("cefi", "trades") == gate.ContinuityAction.AUTOSTART_REPLAY


def test_assert_shard_operable_live_clears_when_confirmed_running(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.LIVE)
    assert (
        gate.assert_shard_operable("cefi", "l2_book", live_already_running=True)
        == gate.ContinuityAction.LIVE_MUST_BE_RUNNING
    )


def test_assert_shard_operable_live_raises_when_not_confirmed_running(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.LIVE)
    with pytest.raises(gate.ShardUnavailableError, match="live is not confirmed running"):
        gate.assert_shard_operable("cefi", "l2_book", live_already_running=False)


def test_assert_shard_operable_wait_for_batch_clears_when_gaps_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.BATCH)
    assert gate.assert_shard_operable("sports", "fixtures", gaps_ok=True) == gate.ContinuityAction.WAIT_FOR_BATCH


def test_assert_shard_operable_wait_for_batch_raises_when_gaps_not_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: mode == Mode.BATCH)
    with pytest.raises(gate.ShardUnavailableError, match="no configured-OK-gap"):
        gate.assert_shard_operable("sports", "fixtures", gaps_ok=False)


def test_assert_shard_operable_nothing_capable_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "could_exist", lambda ag, dt, mode: False)
    with pytest.raises(gate.ShardUnavailableError):
        gate.assert_shard_operable("unregistered", "unknown", gaps_ok=False, live_already_running=True)
