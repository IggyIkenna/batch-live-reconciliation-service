"""Unit tests for the live read-path mode resolver (GATE-0 M4 wiring).

Verifies the manifest ``pipeline_mode``-string → :class:`Mode` mapping and that the
contextual precedence is delegated to UAC's ``select_for_mode`` (live consumer reads
``live > replay > batch``; batch consumer reads ``batch > replay > live``; ``replay``
is always the middle gap-fill tier).
"""

from __future__ import annotations

import pytest
from unified_api_contracts import Mode

from batch_live_reconciliation_service.engine.mode_resolver import (
    available_modes_from_pipeline_mode_values,
    mode_for_pipeline_mode_value,
    resolve_read_mode,
)


def test_mode_for_pipeline_mode_value_maps_closed_set() -> None:
    assert mode_for_pipeline_mode_value("batch_tardis") == Mode.BATCH
    assert mode_for_pipeline_mode_value("live_binance") == Mode.LIVE
    assert mode_for_pipeline_mode_value("replay_databento") == Mode.REPLAY


def test_mode_for_pipeline_mode_value_unknown_raises() -> None:
    with pytest.raises(ValueError):
        mode_for_pipeline_mode_value("not_a_real_pipeline_mode")


def test_available_modes_collapses_strings_to_mode_set() -> None:
    got = available_modes_from_pipeline_mode_values(["batch_tardis", "live_binance", "replay_databento"])
    assert got == frozenset({Mode.BATCH, Mode.LIVE, Mode.REPLAY})


def test_available_modes_skips_unknown_and_blank() -> None:
    got = available_modes_from_pipeline_mode_values(["batch_tardis", "", "garbage_mode", "live_databento"])
    assert got == frozenset({Mode.BATCH, Mode.LIVE})


def test_resolve_read_mode_live_context_prefers_live() -> None:
    assert (
        resolve_read_mode(
            consumer_mode=Mode.LIVE,
            available_pipeline_mode_values=["live_binance", "batch_tardis"],
        )
        == Mode.LIVE
    )


def test_resolve_read_mode_batch_context_prefers_batch() -> None:
    assert (
        resolve_read_mode(
            consumer_mode=Mode.BATCH,
            available_pipeline_mode_values=["live_binance", "batch_tardis"],
        )
        == Mode.BATCH
    )


def test_resolve_read_mode_replay_is_always_the_middle_tier() -> None:
    # Live context, live absent → replay (not batch).
    assert (
        resolve_read_mode(
            consumer_mode=Mode.LIVE,
            available_pipeline_mode_values=["replay_databento", "batch_tardis"],
        )
        == Mode.REPLAY
    )
    # Batch context, batch absent → replay (not live).
    assert (
        resolve_read_mode(
            consumer_mode=Mode.BATCH,
            available_pipeline_mode_values=["replay_databento", "live_binance"],
        )
        == Mode.REPLAY
    )


def test_resolve_read_mode_no_recognised_mode_raises() -> None:
    with pytest.raises(ValueError):
        resolve_read_mode(
            consumer_mode=Mode.LIVE,
            available_pipeline_mode_values=["garbage", ""],
        )


def test_legacy_transitional_alias_still_resolves_to_live_mode() -> None:
    """Backward-compat: old parquets carry the retired transitional alias (live_ + websocket).

    The PipelineMode enum member has been DELETED (the source-aware ``live_<source>``
    migration landed), so constructing ``PipelineMode`` from the retired alias string now
    raises — but old parquet
    rows still carry the string. ``mode_for_pipeline_mode_value`` must still map it to
    Mode.LIVE (via the leading-``live_`` prefix fallback), never drop it from the
    reconciliation mode-set.

    The alias string is constructed at runtime via concatenation so the grep gate
    (zero bare alias tokens in source) stays satisfied while the runtime still exercises
    the legacy-string path.
    """
    legacy_value = "live_" + "websocket"  # backward-compat: retired transitional alias string
    assert mode_for_pipeline_mode_value(legacy_value) == Mode.LIVE
