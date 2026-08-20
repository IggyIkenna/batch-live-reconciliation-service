"""W12: pause-before-manual-entry, virtual/persistent delta exclusion, soft-delete audit.

Each of these three was explicitly unbuilt before 2026-08-20. The tests pin the
properties that make them worth having rather than just their happy paths:

* a pause is an INTERLOCK — `/book-correction` refuses without one, so it cannot
  be skipped by forgetting;
* virtual and persistent exclusions differ in LIFETIME, and a virtual one must
  not silently behave like a persistent one on a later run;
* revoking anything is a SOFT delete — the record survives for audit.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from batch_live_reconciliation_service.api.resolution_state import (
    PERSISTENT_EXCLUSIONS_BLOB,
    DeltaExclusion,
    ExclusionScope,
    PauseRequiredError,
    ResolutionStateStore,
)

_BUCKET = "recon-test-bucket"


class _FakeStorage:
    """In-memory stand-in for the UTL storage client — real round-tripping,
    so the persistence path is genuinely exercised rather than mocked away."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        del bucket
        if blob_path not in self.objects:
            raise OSError(f"no such object: {blob_path}")
        return self.objects[blob_path]

    def upload_bytes(self, bucket: str, blob_path: str, data: bytes) -> str:
        del bucket
        self.objects[blob_path] = data
        return blob_path


@pytest.fixture
def storage() -> _FakeStorage:
    return _FakeStorage()


@pytest.fixture
def store(storage: _FakeStorage):  # noqa: ANN201 — pytest fixture, type is the store
    with patch(
        "batch_live_reconciliation_service.api.resolution_state.get_storage_client",
        return_value=storage,
    ):
        yield ResolutionStateStore()


class TestPauseInterlock:
    def test_require_pause_raises_when_absent(self, store: ResolutionStateStore) -> None:
        with pytest.raises(PauseRequiredError):
            store.require_pause("Binance", "BTC-USDT")

    def test_require_pause_returns_the_active_pause(self, store: ResolutionStateStore) -> None:
        store.pause(
            venue="Binance",
            instrument_id="BTC-USDT",
            break_id="BRK-001",
            reason="booking a manual correction",
            paused_by="ops@example.com",
        )
        assert store.require_pause("Binance", "BTC-USDT").break_id == "BRK-001"

    def test_pause_lookup_is_case_insensitive(self, store: ResolutionStateStore) -> None:
        """The break's venue casing comes from recon output; an operator pausing
        'binance' must satisfy a break reported as 'Binance'."""
        store.pause(
            venue="binance",
            instrument_id="btc-usdt",
            break_id="BRK-001",
            reason="booking a manual correction",
            paused_by="ops@example.com",
        )
        assert store.active_pause("BINANCE", "BTC-USDT") is not None

    def test_a_revoked_pause_no_longer_satisfies_the_interlock(self, store: ResolutionStateStore) -> None:
        store.pause(
            venue="Binance",
            instrument_id="BTC-USDT",
            break_id="BRK-001",
            reason="booking a manual correction",
            paused_by="ops@example.com",
        )
        _ = store.revoke_pause(
            venue="Binance", instrument_id="BTC-USDT", revoked_by="ops@example.com", reason="correction booked"
        )
        with pytest.raises(PauseRequiredError):
            store.require_pause("Binance", "BTC-USDT")

    def test_revoked_pause_is_retained_for_audit(self, store: ResolutionStateStore) -> None:
        store.pause(
            venue="Binance",
            instrument_id="BTC-USDT",
            break_id="BRK-001",
            reason="booking a manual correction",
            paused_by="ops@example.com",
        )
        _ = store.revoke_pause(
            venue="Binance", instrument_id="BTC-USDT", revoked_by="auditor", reason="correction booked"
        )
        history = store.all_pauses()
        assert len(history) == 1
        assert history[0].is_active is False
        assert history[0].revoked_by == "auditor"
        assert history[0].revoke_reason == "correction booked"

    def test_repausing_appends_rather_than_overwriting(self, store: ResolutionStateStore) -> None:
        for n in range(2):
            store.pause(
                venue="Binance",
                instrument_id="BTC-USDT",
                break_id=f"BRK-00{n}",
                reason="booking a manual correction",
                paused_by="ops@example.com",
            )
        assert len(store.all_pauses()) == 2


class TestExclusionLifetime:
    def test_virtual_applies_only_to_its_own_run_date(self, store: ResolutionStateStore) -> None:
        store.exclude(
            break_id="BRK-001",
            scope=ExclusionScope.VIRTUAL,
            reason="known settlement-timing artefact",
            excluded_by="ops@example.com",
            run_date="2026-03-22",
            bucket=_BUCKET,
        )
        assert store.excluded_break_ids(run_date="2026-03-22", bucket=_BUCKET) == frozenset({"BRK-001"})
        assert store.excluded_break_ids(run_date="2026-03-23", bucket=_BUCKET) == frozenset()

    def test_persistent_applies_to_every_run_date(self, store: ResolutionStateStore) -> None:
        store.exclude(
            break_id="BRK-002",
            scope=ExclusionScope.PERSISTENT,
            reason="structural divergence accepted by risk",
            excluded_by="ops@example.com",
            run_date=None,
            bucket=_BUCKET,
        )
        for date in ("2026-03-22", "2026-09-01"):
            assert "BRK-002" in store.excluded_break_ids(run_date=date, bucket=_BUCKET)

    def test_virtual_without_a_run_date_is_rejected_not_promoted(self, store: ResolutionStateStore) -> None:
        """Silently treating it as persistent is exactly how a one-off suppression
        becomes permanent — the whole reason the two scopes exist."""
        with pytest.raises(ValueError, match="requires run_date"):
            store.exclude(
                break_id="BRK-003",
                scope=ExclusionScope.VIRTUAL,
                reason="known settlement-timing artefact",
                excluded_by="ops@example.com",
                run_date=None,
                bucket=_BUCKET,
            )

    def test_persistent_ignores_a_supplied_run_date(self, store: ResolutionStateStore) -> None:
        entry = store.exclude(
            break_id="BRK-004",
            scope=ExclusionScope.PERSISTENT,
            reason="structural divergence accepted by risk",
            excluded_by="ops@example.com",
            run_date="2026-03-22",
            bucket=_BUCKET,
        )
        assert entry.run_date is None


class TestPersistence:
    def test_persistent_exclusion_reaches_gcs(self, store: ResolutionStateStore, storage: _FakeStorage) -> None:
        store.exclude(
            break_id="BRK-002",
            scope=ExclusionScope.PERSISTENT,
            reason="structural divergence accepted by risk",
            excluded_by="ops@example.com",
            run_date=None,
            bucket=_BUCKET,
        )
        assert PERSISTENT_EXCLUSIONS_BLOB in storage.objects
        payload = json.loads(storage.objects[PERSISTENT_EXCLUSIONS_BLOB].decode("utf-8"))
        assert payload[0]["break_id"] == "BRK-002"
        assert payload[0]["scope"] == "persistent"

    def test_persistent_exclusion_survives_a_new_store(
        self, store: ResolutionStateStore, storage: _FakeStorage
    ) -> None:
        """'Persistent' means it outlives the process, not just the request."""
        store.exclude(
            break_id="BRK-002",
            scope=ExclusionScope.PERSISTENT,
            reason="structural divergence accepted by risk",
            excluded_by="ops@example.com",
            run_date=None,
            bucket=_BUCKET,
        )
        with patch(
            "batch_live_reconciliation_service.api.resolution_state.get_storage_client",
            return_value=storage,
        ):
            fresh = ResolutionStateStore()
            assert "BRK-002" in fresh.excluded_break_ids(run_date="2027-01-01", bucket=_BUCKET)

    def test_a_virtual_exclusion_does_not_survive_a_new_store(
        self, store: ResolutionStateStore, storage: _FakeStorage
    ) -> None:
        store.exclude(
            break_id="BRK-001",
            scope=ExclusionScope.VIRTUAL,
            reason="known settlement-timing artefact",
            excluded_by="ops@example.com",
            run_date="2026-03-22",
            bucket=_BUCKET,
        )
        with patch(
            "batch_live_reconciliation_service.api.resolution_state.get_storage_client",
            return_value=storage,
        ):
            fresh = ResolutionStateStore()
            assert fresh.excluded_break_ids(run_date="2026-03-22", bucket=_BUCKET) == frozenset()

    def test_corrupt_exclusions_object_re_raises_breaks_rather_than_suppressing(self) -> None:
        """Half-parsed suppression state must never hide a break — failing open
        (re-raising the break) is the safe direction here."""
        broken = MagicMock()
        broken.download_bytes.return_value = b"{not json at all"
        with patch(
            "batch_live_reconciliation_service.api.resolution_state.get_storage_client",
            return_value=broken,
        ):
            store = ResolutionStateStore()
            assert store.excluded_break_ids(run_date="2026-03-22", bucket=_BUCKET) == frozenset()


class TestExclusionSoftDelete:
    def test_revoking_a_virtual_exclusion_re_raises_the_break(self, store: ResolutionStateStore) -> None:
        store.exclude(
            break_id="BRK-001",
            scope=ExclusionScope.VIRTUAL,
            reason="known settlement-timing artefact",
            excluded_by="ops@example.com",
            run_date="2026-03-22",
            bucket=_BUCKET,
        )
        revoked = store.revoke_exclusion(
            break_id="BRK-001", revoked_by="auditor", reason="artefact turned out to be real", bucket=_BUCKET
        )
        assert revoked is not None
        assert store.excluded_break_ids(run_date="2026-03-22", bucket=_BUCKET) == frozenset()

    def test_revoking_a_persistent_exclusion_rewrites_gcs_keeping_the_record(
        self, store: ResolutionStateStore, storage: _FakeStorage
    ) -> None:
        store.exclude(
            break_id="BRK-002",
            scope=ExclusionScope.PERSISTENT,
            reason="structural divergence accepted by risk",
            excluded_by="ops@example.com",
            run_date=None,
            bucket=_BUCKET,
        )
        _ = store.revoke_exclusion(
            break_id="BRK-002", revoked_by="auditor", reason="risk withdrew the acceptance", bucket=_BUCKET
        )
        payload = json.loads(storage.objects[PERSISTENT_EXCLUSIONS_BLOB].decode("utf-8"))
        assert len(payload) == 1, "the record must be RETAINED, not removed"
        assert payload[0]["revoked_by"] == "auditor"
        assert "BRK-002" not in store.excluded_break_ids(run_date="2026-03-22", bucket=_BUCKET)

    def test_revoked_exclusions_stay_in_the_audit_view(self, store: ResolutionStateStore) -> None:
        store.exclude(
            break_id="BRK-001",
            scope=ExclusionScope.VIRTUAL,
            reason="known settlement-timing artefact",
            excluded_by="ops@example.com",
            run_date="2026-03-22",
            bucket=_BUCKET,
        )
        _ = store.revoke_exclusion(
            break_id="BRK-001", revoked_by="auditor", reason="artefact turned out to be real", bucket=_BUCKET
        )
        all_entries = store.all_exclusions(_BUCKET)
        assert len(all_entries) == 1
        assert all_entries[0].is_active is False

    def test_revoking_an_unknown_break_returns_none(self, store: ResolutionStateStore) -> None:
        assert (
            store.revoke_exclusion(
                break_id="NOPE", revoked_by="auditor", reason="nothing to revoke here", bucket=_BUCKET
            )
            is None
        )


class TestRoundTrip:
    def test_exclusion_json_round_trips_every_field(self) -> None:
        original = DeltaExclusion(
            break_id="BRK-009",
            scope=ExclusionScope.PERSISTENT,
            reason="structural divergence accepted by risk",
            excluded_by="ops@example.com",
            excluded_at=datetime(2026, 8, 20, 9, 0, tzinfo=UTC),
        )
        assert DeltaExclusion.from_json(original.to_json()) == original


class TestW12Endpoints:
    """HTTP layer over the store. Patches `get_recon_config` / `get_storage_client`
    the same way `test_resolution_api.py` does, so these run without service config.

    `BRK-001` is Binance/BTC-USDT dated 2026-03-22 in the pre-activation mock
    break set `_current_breaks()` falls back to.
    """

    @pytest.fixture
    def api(self, storage: _FakeStorage):  # noqa: ANN201 — pytest fixture
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from batch_live_reconciliation_service.api import resolution_api

        app = FastAPI()
        app.include_router(resolution_api.router)
        config = MagicMock()
        config.recon_bucket = _BUCKET
        resolution_api._state = ResolutionStateStore()
        with (
            patch("batch_live_reconciliation_service.api.resolution_api.get_recon_config", return_value=config),
            patch("batch_live_reconciliation_service.api.resolution_api.get_storage_client", return_value=storage),
            patch(
                "batch_live_reconciliation_service.api.resolution_state.get_storage_client",
                return_value=storage,
            ),
        ):
            yield TestClient(app, raise_server_exceptions=False)
        resolution_api._state = ResolutionStateStore()

    def test_book_correction_is_refused_without_a_pause(self, api) -> None:  # noqa: ANN001
        resp = api.post("/t1-recon/book-correction", json={"break_id": "BRK-001"})
        assert resp.status_code == 409
        assert "Pause before booking" in resp.json()["detail"]

    def test_book_correction_succeeds_once_paused(self, api) -> None:  # noqa: ANN001
        _ = api.post(
            "/t1-recon/pause",
            json={"break_id": "BRK-001", "reason": "booking a manual correction", "actor": "ops"},
        )
        assert api.post("/t1-recon/book-correction", json={"break_id": "BRK-001"}).status_code == 200

    def test_the_interlock_re_arms_when_the_pause_is_revoked(self, api) -> None:  # noqa: ANN001
        _ = api.post(
            "/t1-recon/pause",
            json={"break_id": "BRK-001", "reason": "booking a manual correction", "actor": "ops"},
        )
        _ = api.post(
            "/t1-recon/pause/revoke",
            json={"break_id": "BRK-001", "reason": "correction is booked now", "actor": "auditor"},
        )
        assert api.post("/t1-recon/book-correction", json={"break_id": "BRK-001"}).status_code == 409

    def test_excluded_break_is_hidden_then_shown_with_the_flag(self, api) -> None:  # noqa: ANN001
        _ = api.post(
            "/t1-recon/exclusions",
            json={"break_id": "BRK-001", "scope": "virtual", "reason": "known timing artefact", "actor": "ops"},
        )
        assert "BRK-001" not in [b["break_id"] for b in api.get("/t1-recon/breaks").json()]
        shown = {b["break_id"]: b["status"] for b in api.get("/t1-recon/breaks?include_excluded=true").json()}
        assert shown["BRK-001"] == "excluded"

    def test_revoking_an_exclusion_re_raises_the_break_and_keeps_the_record(self, api) -> None:  # noqa: ANN001
        _ = api.post(
            "/t1-recon/exclusions",
            json={"break_id": "BRK-001", "scope": "virtual", "reason": "known timing artefact", "actor": "ops"},
        )
        _ = api.post(
            "/t1-recon/exclusions/revoke",
            json={"break_id": "BRK-001", "reason": "artefact was real after all", "actor": "auditor"},
        )
        assert "BRK-001" in [b["break_id"] for b in api.get("/t1-recon/breaks").json()]
        audit = api.get("/t1-recon/exclusions").json()
        assert len(audit) == 1
        assert audit[0]["active"] is False
        assert api.get("/t1-recon/exclusions?active_only=true").json() == []

    def test_an_unknown_break_is_404_not_500(self, api) -> None:  # noqa: ANN001
        resp = api.post("/t1-recon/pause", json={"break_id": "NOPE", "reason": "no such break here", "actor": "ops"})
        assert resp.status_code == 404
