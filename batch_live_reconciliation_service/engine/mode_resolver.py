"""Live read-path mode resolver — wires UAC's mode-contextual precedence (GATE-0 M4).

The cross-mode UNION reconciliation can populate a single cell in more than one
:class:`~unified_api_contracts.canonical.crosscutting.pipeline_mode.Mode`
(``batch`` / ``live`` / ``replay``). When a reader consumes that cell it must pick
exactly ONE mode's value. This module is the thin read-path adapter that, given the
manifest's available ``pipeline_mode`` strings for a cell plus the consumer's
mode-context, defers the actual precedence decision to UAC's
:func:`unified_api_contracts.select_for_mode` (the SSOT for the contextual order —
``live`` consumer reads ``live > replay > batch``; ``batch`` consumer reads
``batch > replay > live``; ``replay`` is always the middle gap-fill tier).

Keeping the precedence logic in UAC (not re-implemented here) means batch-live-
reconciliation-service and every other read-path consumer agree on the same order.
This module only does the manifest-string → :class:`Mode` mapping that is specific
to how this service reads the manifest ``pipeline_mode`` column.
"""

from __future__ import annotations

from collections.abc import Iterable

from unified_api_contracts import (
    Mode,
    PipelineMode,
    mode_of,
    select_for_mode,
)


def mode_for_pipeline_mode_value(pipeline_mode_value: str) -> Mode:
    """Map a raw manifest ``pipeline_mode`` column value to its abstract :class:`Mode`.

    The manifest stores ``pipeline_mode`` as a closed-set string (e.g. ``batch_tardis``,
    ``live_binance``, ``replay_databento``). This resolves it to the abstract
    :class:`Mode` via the UAC closed set — never by ad-hoc prefix string matching, so
    the mode axis stays bound to the UAC SSOT.

    :raises ValueError: if ``pipeline_mode_value`` is not a known :class:`PipelineMode`.
    """
    return mode_of(PipelineMode(pipeline_mode_value))


def available_modes_from_pipeline_mode_values(
    pipeline_mode_values: Iterable[str],
) -> frozenset[Mode]:
    """Collapse a cell's available ``pipeline_mode`` strings into the set of :class:`Mode`.

    Unknown / unparseable values are skipped (the cell may carry a legacy or future
    ``pipeline_mode`` this service does not model) — only recognised modes contribute
    to the precedence decision.
    """
    modes: set[Mode] = set()
    for value in pipeline_mode_values:
        if not value:
            continue
        try:
            modes.add(mode_for_pipeline_mode_value(value))
        except ValueError:
            # Not a known PipelineMode — skip; do not let one bad row break resolution.
            continue
    return frozenset(modes)


def resolve_read_mode(
    *,
    consumer_mode: Mode,
    available_pipeline_mode_values: Iterable[str],
) -> Mode:
    """Return the :class:`Mode` a ``consumer_mode``-context reader should consume.

    Maps the cell's available manifest ``pipeline_mode`` strings to the abstract
    :class:`Mode` set, then defers to UAC's :func:`select_for_mode` for the
    mode-contextual precedence (``replay`` always the middle gap-fill tier).

    :raises ValueError: if no recognised mode is available for the cell, or none
        matches the consumer's context-order (propagated from ``select_for_mode``).
    """
    available_modes = available_modes_from_pipeline_mode_values(available_pipeline_mode_values)
    return select_for_mode(consumer_mode, available_modes)


__all__ = [
    "available_modes_from_pipeline_mode_values",
    "mode_for_pipeline_mode_value",
    "resolve_read_mode",
]
