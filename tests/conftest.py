"""Shared pytest fixtures for batch-live-reconciliation-service."""

from __future__ import annotations

import pytest
from unified_trading_library import MockEventSink, setup_events


@pytest.fixture(autouse=True, scope="session")
def _init_event_logging() -> None:
    """Initialize event logging once per test session using MockEventSink."""
    setup_events(
        service_name="batch-live-reconciliation-service",
        mode="test",
        sink=MockEventSink(),
    )
