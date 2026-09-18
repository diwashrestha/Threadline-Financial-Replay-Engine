# Additional imports for the scenario tests.
# Imports already present in this file do not need to be duplicated.

import hashlib
import json
import random

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from threadline.archival import ArchiveService, sha256_file
from threadline.canonicalize import RecordEnvelope, canonicalize
from threadline.completeness import REPORT_SOURCES
from threadline.contracts import (
    ContractViolation,
    EntityType,
    MovementType,
    ReportType,
    SourceCompleteness,
    parse_source_record,
    quarantine_from_violation,
)
from threadline.publication import validate_for_publication
from threadline.reconcile import reconcile
from threadline.recovery_adapters import (
    build_candidate,
    load_completeness,
)
from threadline.recovery_fingerprint import (
    assert_financial_equal,
    document_fingerprint,
    logical_fingerprint,
)
from threadline.source_files import (
    FileGateState,
    ingest_ready_source,
    publish_source_file,
)


SCENARIO_REPORT_DATE = date(2026, 9, 14)

SCENARIO_REPORT_TYPES = {
    EntityType.PAYMENT: ReportType.PAYMENTS,
    EntityType.REFUND: ReportType.REFUNDS,
}


def scenario_payment(
    *,
    amount="100.00",
    source_version=1,
    available_on="2026-09-14",
):
    return {
        "payment_id": "PAY-RECOVERY-001",
        "order_id": "ORD-RECOVERY-001",
        "attempt_number": 1,
        "payment_method": "CARD",
        "status": "CAPTURED",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": "2026-09-14T08:00:00Z",
        "available_on": available_on,
        "source_version": source_version,
    }


def scenario_refund(
    *,
    amount="25.00",
    source_version=1,
    available_on="2026-09-14",
):
    return {
        "refund_id": "REF-RECOVERY-001",
        "payment_id": "PAY-RECOVERY-001",
        "status": "SUCCEEDED",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": "2026-09-14T09:00:00Z",
        "available_on": available_on,
        "source_version": source_version,
    }


def scenario_worker(database_url, *, failure_hook=None):
    return FullRebuildRecovery(
        database_url=database_url,
        build_candidate=build_candidate,
        validate_domain=validate_for_publication,
        failure_hook=failure_hook,
    )


@dataclass(frozen=True)
class ScenarioDelivery:
    batch_id: UUID
    request_id: UUID
    receipt_ids: tuple[UUID, ...]
    source_path: Path


@dataclass
class RecoveryScenario:
    database_url: str
    inbox_root: Path
    archive_root: Path
    envelopes: list = field(default_factory=list)
    quarantine: list = field(default_factory=list)
    delivery_count: int = 0

    def deliver(
        self,
        entity_type,
        records,
        *,
        filename=None,
        disposition="ACCEPTED",
    ):
        """Seed complete source evidence and enqueue verification recovery.

        This is a test fixture, not the production ingestion implementation.

        Every delivery forces a FULL request so duplicate and stale
        deliveries are tested by an actual rebuild too.
        """
        self.delivery_count += 1

        report_type = SCENARIO_REPORT_TYPES[entity_type]
        source_system = REPORT_SOURCES[report_type]

        if filename is None:
            filename = (
                f"{report_type.value}-{self.delivery_count:03d}.json"
            )

        source_path = self.inbox_root / filename

        manifest_path = publish_source_file(
            source_path,
            records=records,
            source_system=source_system.value,
            report_type=report_type.name,
            report_date=SCENARIO_REPORT_DATE,
            entity_type=entity_type.name,
        )

        file_bytes = source_path.read_bytes()
        manifest_bytes = manifest_path.read_bytes()

        file_checksum = hashlib.sha256(file_bytes).hexdigest()
        manifest_checksum = hashlib.sha256(manifest_bytes).hexdigest()

        delivery_key = document_fingerprint(
            {
                "filename": filename,
                "file_checksum": file_checksum,
                "manifest_checksum": manifest_checksum,
                "entity_type": entity_type.value,
            }
        )

        pending_envelopes = []
        pending_quarantine = []
        receipt_ids = []

        with psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        ) as connection:
            lock_financial_state(connection)

            batch = connection.execute(
                """
                INSERT INTO ingestion_batch (
                    delivery_key,
                    source_system,
                    report_type,
                    report_date,
                    original_filename,
                    file_checksum,
                    manifest_checksum,
                    schema_version,
                    declared_row_count,
                    observed_row_count,
                    ingestion_status,
                    source_relative_path
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    '1', %s, %s, 'PROCESSING', %s
                )
                RETURNING batch_id
                """,
                (
                    delivery_key,
                    source_system.value,
                    report_type.name,
                    SCENARIO_REPORT_DATE,
                    filename,
                    file_checksum,
                    manifest_checksum,
                    len(records),
                    len(records),
                    filename,
                ),
            ).fetchone()

            batch_id = batch["batch_id"]

            for row_number, payload in enumerate(records, start=1):
                record = None
                rejected = None

                try:
                    record = parse_source_record(entity_type, payload)
                except ContractViolation as violation:
                    rejected = quarantine_from_violation(
                        entity_type,
                        payload,
                        violation,
                    )

                if rejected is not None:
                    source_id = rejected.source_id
                    source_version = payload.get("source_version")
                    stored_disposition = "QUARANTINED"
                    reason_code = rejected.reason_code

                    payload_hash = document_fingerprint(payload)
                else:
                    source_id = record.record_id
                    source_version = record.source_version
                    stored_disposition = disposition
                    reason_code = None

                    # RecordEnvelope computes the canonical payload hash.
                    payload_hash = RecordEnvelope(
                        receipt_id="fixture-hash",
                        record=record,
                    ).payload_hash

                receipt = connection.execute(
                    """
                    INSERT INTO source_receipt (
                        batch_id,
                        row_number,
                        entity_type,
                        source_id,
                        source_version,
                        payload_hash,
                        raw_payload,
                        disposition,
                        reason_code
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING receipt_id
                    """,
                    (
                        batch_id,
                        row_number,
                        entity_type.value,
                        source_id,
                        source_version,
                        payload_hash,
                        Jsonb(payload),
                        stored_disposition,
                        reason_code,
                    ),
                ).fetchone()

                receipt_id = receipt["receipt_id"]
                receipt_ids.append(receipt_id)

                if rejected is not None:
                    pending_quarantine.append(rejected)
                else:
                    pending_envelopes.append(
                        RecordEnvelope(
                            receipt_id=str(receipt_id),
                            record=record,
                        )
                    )

            connection.execute(
                """
                UPDATE ingestion_batch
                SET
                    ingestion_status = 'COMMITTED',
                    committed_at_utc = clock_timestamp()
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            frozen_time = connection.execute(
                "SELECT clock_timestamp() AS value"
            ).fetchone()["value"]

            request_id = enqueue_full_rebuild(
                connection,
                trigger_receipt_id=receipt_ids[0],
                canonical_changed=True,
                affects_published_history=True,
                change_key=f"scenario-verification:{batch_id}",
                reason_code="SCENARIO_VERIFICATION",
                as_of_utc=frozen_time,
            )

        # The connection context has committed before fixture state advances.
        self.envelopes.extend(pending_envelopes)
        self.quarantine.extend(pending_quarantine)

        assert request_id is not None

        return ScenarioDelivery(
            batch_id=batch_id,
            request_id=request_id,
            receipt_ids=tuple(receipt_ids),
            source_path=source_path,
        )

    def publication(self):
        with psycopg.connect(self.database_url) as connection:
            return read_published_result(connection)

    def recover_next(self, *, failure_hook=None):
        outcome = scenario_worker(
            self.database_url,
            failure_hook=failure_hook,
        ).run_next()

        assert outcome is not None
        self.assert_matches_rebuild(outcome)

        return outcome

    def assert_matches_rebuild(self, outcome):
        """Compare database reconstruction with full replay of fixture inputs."""
        with psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        ) as connection:
            request = connection.execute(
                """
                SELECT as_of_utc
                FROM recovery_request
                WHERE request_id = %s
                """,
                (outcome.request_id,),
            ).fetchone()

            # Quality evaluation is shared; source reconstruction is independent.
            completeness = load_completeness(
                connection,
                request["as_of_utc"],
            )

        ordered_envelopes = sorted(
            self.envelopes,
            key=lambda envelope: (
                envelope.record.ENTITY_TYPE.value,
                envelope.record.record_id,
                envelope.record.source_version,
                envelope.payload_hash,
                envelope.receipt_id,
            ),
        )

        reference = reconcile(
            run_id=str(uuid4()),
            detected_at=request["as_of_utc"],
            canonicalization=canonicalize(ordered_envelopes),
            completeness_results=completeness,
            quarantine_records=tuple(self.quarantine),
        )

        publication = self.publication()

        assert publication is not None
        assert publication["current_run_id"] == outcome.run_id

        assert_financial_equal(
            publication["result_payload"],
            reference,
        )

        assert publication["logical_fingerprint"] == (
            logical_fingerprint(reference)
        )

        assert verify_stored_financial_result(publication) == (
            outcome.logical_fingerprint
        )

    def assert_evidence_conserved(self):
        with psycopg.connect(self.database_url) as connection:
            rows = connection.execute(
                """
                SELECT
                    b.batch_id,
                    b.observed_row_count,
                    COUNT(r.receipt_id)
                FROM ingestion_batch AS b
                LEFT JOIN source_receipt AS r
                    ON r.batch_id = b.batch_id
                GROUP BY b.batch_id, b.observed_row_count
                """
            ).fetchall()

        assert rows
        assert all(observed == stored for _, observed, stored in rows)


@pytest.fixture
def recovery_scenario(database_url, tmp_path):
    inbox = tmp_path / "inbox"
    archive = tmp_path / "archive"

    inbox.mkdir()

    return RecoveryScenario(
        database_url=database_url,
        inbox_root=inbox,
        archive_root=archive,
    )


def scenario_assert_money(
    publication,
    *,
    captured,
    fees,
    refunded,
):
    """Assert independent monetary expectations, not only matching hashes."""
    rows = publication["result_payload"]["expected_movements"]

    def total(movement_type):
        return sum(
            (
                Decimal(str(row["signed_amount"]))
                for row in rows
                if row["movement_type"] == movement_type.value
            ),
            Decimal("0.00"),
        )

    assert total(MovementType.CAPTURE) == Decimal(captured)
    assert total(MovementType.FEE) == -Decimal(fees)
    assert total(MovementType.REFUND) == -Decimal(refunded)

    net = sum(
        (Decimal(str(row["signed_amount"])) for row in rows),
        Decimal("0.00"),
    )

    assert net == (
        Decimal(captured)
        - Decimal(fees)
        - Decimal(refunded)
    )


def scenario_make_retry_due(database_url, request_id):
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_request
            SET next_attempt_at_utc =
                clock_timestamp() - interval '1 second'
            WHERE request_id = %s
              AND status = 'PENDING'
            """,
            (request_id,),
        )


def test_scenario_late_refund_and_duplicate_replay(recovery_scenario):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    baseline = case.recover_next()

    scenario_assert_money(
        case.publication(),
        captured="100.00",
        fees="2.00",
        refunded="0.00",
    )

    late = case.deliver(
        EntityType.REFUND,
        [scenario_refund()],
        filename="refund-late.json",
    )

    # The new receipt arrived after the baseline input watermark.
    with psycopg.connect(case.database_url) as connection:
        arrived_after_baseline = connection.execute(
            """
            SELECT r.received_at_utc > run.input_watermark_utc
            FROM source_receipt AS r
            CROSS JOIN reconciliation_run AS run
            WHERE r.receipt_id = %s
              AND run.run_id = %s
            """,
            (late.receipt_ids[0], baseline.run_id),
        ).fetchone()[0]

    assert arrived_after_baseline is True

    recovered = case.recover_next()

    scenario_assert_money(
        case.publication(),
        captured="100.00",
        fees="2.00",
        refunded="25.00",
    )

    assert recovered.logical_fingerprint != baseline.logical_fingerprint

    case.deliver(
        EntityType.REFUND,
        [scenario_refund()],
        filename="refund-redelivered.json",
        disposition="DUPLICATE",
    )

    duplicate = case.recover_next()

    assert duplicate.run_id != recovered.run_id
    assert duplicate.logical_fingerprint == recovered.logical_fingerprint
    assert duplicate.input_fingerprint != recovered.input_fingerprint

    case.assert_evidence_conserved()


def test_scenario_identical_payment_new_filename_is_safe(recovery_scenario):
    case = recovery_scenario

    case.deliver(
        EntityType.PAYMENT,
        [scenario_payment()],
        filename="payment-original.json",
    )
    first = case.recover_next()

    case.deliver(
        EntityType.PAYMENT,
        [scenario_payment()],
        filename="payment-replayed.json",
        disposition="DUPLICATE",
    )
    second = case.recover_next()

    assert first.logical_fingerprint == second.logical_fingerprint
    assert first.input_fingerprint != second.input_fingerprint

    scenario_assert_money(
        case.publication(),
        captured="100.00",
        fees="2.00",
        refunded="0.00",
    )

    case.assert_evidence_conserved()


def test_scenario_correction_wins_over_later_stale_replay(recovery_scenario):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    original = case.recover_next()

    case.deliver(
        EntityType.PAYMENT,
        [scenario_payment(amount="125.00", source_version=2)],
    )
    corrected = case.recover_next()

    assert corrected.logical_fingerprint != original.logical_fingerprint

    scenario_assert_money(
        case.publication(),
        captured="125.00",
        fees="2.45",
        refunded="0.00",
    )

    case.deliver(
        EntityType.PAYMENT,
        [scenario_payment()],
        disposition="STALE",
    )
    stale_replay = case.recover_next()

    assert stale_replay.logical_fingerprint == corrected.logical_fingerprint

    case.assert_evidence_conserved()


def test_scenario_conflict_excludes_money_and_preserves_variants(
    recovery_scenario,
):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    baseline = case.recover_next()

    case.deliver(
        EntityType.PAYMENT,
        [scenario_payment(amount="120.00", source_version=1)],
        disposition="CONFLICTED",
    )
    conflicted = case.recover_next()

    assert conflicted.logical_fingerprint != baseline.logical_fingerprint

    scenario_assert_money(
        case.publication(),
        captured="0.00",
        fees="0.00",
        refunded="0.00",
    )

    with psycopg.connect(case.database_url) as connection:
        count, variants = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT payload_hash)
            FROM source_receipt
            WHERE entity_type = %s
              AND source_id = 'PAY-RECOVERY-001'
              AND source_version = 1
            """,
            (EntityType.PAYMENT.value,),
        ).fetchone()

    assert count == 2
    assert variants == 2

    # A unique higher version restores an accepted financial fact.
    case.deliver(
        EntityType.PAYMENT,
        [scenario_payment(amount="125.00", source_version=2)],
    )
    case.recover_next()

    scenario_assert_money(
        case.publication(),
        captured="125.00",
        fees="2.45",
        refunded="0.00",
    )

    case.assert_evidence_conserved()


def test_scenario_date_correction_removes_old_movements(recovery_scenario):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    case.recover_next()

    case.deliver(
        EntityType.PAYMENT,
        [
            scenario_payment(
                source_version=2,
                available_on="2026-09-15",
            )
        ],
    )
    case.recover_next()

    movements = case.publication()["result_payload"]["expected_movements"]

    assert len(movements) == 2
    assert {row["available_on"] for row in movements} == {"2026-09-15"}

    scenario_assert_money(
        case.publication(),
        captured="100.00",
        fees="2.00",
        refunded="0.00",
    )


@pytest.mark.parametrize("seed", [0, 7, 42])
def test_scenario_shuffled_arrivals_equal_full_rebuild(
    recovery_scenario,
    seed,
):
    case = recovery_scenario

    arrivals = [
        (EntityType.PAYMENT, scenario_payment()),
        (
            EntityType.PAYMENT,
            scenario_payment(amount="125.00", source_version=2),
        ),
        (EntityType.REFUND, scenario_refund()),
        (
            EntityType.PAYMENT,
            scenario_payment(amount="125.00", source_version=2),
        ),
    ]

    random.Random(seed).shuffle(arrivals)

    for entity_type, payload in arrivals:
        case.deliver(entity_type, [payload])

    completed = []

    while True:
        outcome = scenario_worker(case.database_url).run_next()

        if outcome is None:
            break

        completed.append(outcome)

    assert len(completed) == len(arrivals)

    case.assert_matches_rebuild(completed[-1])

    scenario_assert_money(
        case.publication(),
        captured="125.00",
        fees="2.45",
        refunded="25.00",
    )

    case.assert_evidence_conserved()


@pytest.mark.parametrize(
    "point",
    [
        point
        for point in FailurePoint
        if point is not FailurePoint.AFTER_COMMIT
    ],
    ids=lambda point: point.value,
)
def test_scenario_real_candidate_failure_preserves_previous_result(
    recovery_scenario,
    point,
):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    case.recover_next()

    previous = case.publication()

    late = case.deliver(EntityType.REFUND, [scenario_refund()])
    injector = FailOnce(point)

    with pytest.raises(InjectedFailure, match=point.value):
        scenario_worker(
            case.database_url,
            failure_hook=injector,
        ).run_next()

    assert case.publication() == previous

    observation = inspect_recovery(
        case.database_url,
        late.request_id,
    )

    assert observation.request_status == "PENDING"
    assert observation.attempt_count == 1
    assert observation.result_run_id is None

    case.assert_evidence_conserved()

    with psycopg.connect(case.database_url) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM reconciliation_run"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM recovery_result"
        ).fetchone()[0] == 1

    scenario_make_retry_due(case.database_url, late.request_id)

    recovered = case.recover_next(failure_hook=injector)

    assert recovered.request_id == late.request_id

    scenario_assert_money(
        case.publication(),
        captured="100.00",
        fees="2.00",
        refunded="25.00",
    )

    observation = inspect_recovery(
        case.database_url,
        late.request_id,
    )

    assert observation.request_status == "SUCCEEDED"
    assert observation.attempt_count == 2


def test_scenario_lost_acknowledgement_does_not_publish_twice(
    recovery_scenario,
):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    case.recover_next()

    late = case.deliver(EntityType.REFUND, [scenario_refund()])
    injector = FailOnce(FailurePoint.AFTER_COMMIT)

    with pytest.raises(InjectedFailure, match="after_commit"):
        scenario_worker(
            case.database_url,
            failure_hook=injector,
        ).run_next()

    committed = case.publication()

    scenario_assert_money(
        committed,
        captured="100.00",
        fees="2.00",
        refunded="25.00",
    )

    observation = inspect_recovery(
        case.database_url,
        late.request_id,
    )

    assert observation.action is RecoveryAction.DO_NOT_REPEAT
    assert observation.attempt_count == 1

    assert scenario_worker(case.database_url).run_next() is None
    assert case.publication() == committed

    with psycopg.connect(case.database_url) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM reconciliation_run"
        ).fetchone()[0] == 2


def test_scenario_future_retry_does_not_replace_publication(
    recovery_scenario,
):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    case.recover_next()

    previous = case.publication()
    late = case.deliver(EntityType.REFUND, [scenario_refund()])

    with psycopg.connect(case.database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_request
            SET next_attempt_at_utc =
                clock_timestamp() + interval '1 day'
            WHERE request_id = %s
            """,
            (late.request_id,),
        )

    assert scenario_worker(case.database_url).run_next() is None
    assert case.publication() == previous

    observation = inspect_recovery(
        case.database_url,
        late.request_id,
    )

    assert observation.action is RecoveryAction.WAIT_FOR_RETRY
    assert observation.attempt_count == 0


def test_scenario_retry_limit_preserves_previous_financial_result(
    recovery_scenario,
):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    case.recover_next()

    previous = case.publication()
    late = case.deliver(EntityType.REFUND, [scenario_refund()])

    with psycopg.connect(case.database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_request
            SET max_attempts = 1
            WHERE request_id = %s
            """,
            (late.request_id,),
        )

    with pytest.raises(InjectedFailure):
        scenario_worker(
            case.database_url,
            failure_hook=FailOnce(FailurePoint.BEFORE_COMMIT),
        ).run_next()

    observation = inspect_recovery(
        case.database_url,
        late.request_id,
    )

    assert observation.request_status == "FAILED"
    assert observation.action is RecoveryAction.MANUAL_REQUEUE

    assert scenario_worker(case.database_url).run_next() is None
    assert case.publication() == previous


def test_scenario_archive_failure_preserves_finances_and_retries(
    recovery_scenario,
):
    case = recovery_scenario

    delivery = case.deliver(
        EntityType.PAYMENT,
        [scenario_payment()],
    )
    outcome = case.recover_next()

    previous = case.publication()
    original_checksum = sha256_file(delivery.source_path)

    fired = False

    def archive_failure(point):
        nonlocal fired

        if point == "after_source_remove" and not fired:
            fired = True
            raise RuntimeError("Archive failure after source removal")

    archive = ArchiveService(
        database_url=case.database_url,
        inbox_root=case.inbox_root,
        archive_root=case.archive_root,
        failure_hook=archive_failure,
    )

    with pytest.raises(RuntimeError, match="Archive failure"):
        archive.archive_batch(delivery.batch_id)

    assert fired is True

    # The archival implementation restores the source from its verified copy.
    assert delivery.source_path.exists()
    assert sha256_file(delivery.source_path) == original_checksum
    assert case.publication() == previous

    with psycopg.connect(case.database_url) as connection:
        ingestion_status, archive_status = connection.execute(
            """
            SELECT ingestion_status, archive_status
            FROM ingestion_batch
            WHERE batch_id = %s
            """,
            (delivery.batch_id,),
        ).fetchone()

    assert ingestion_status == "COMMITTED"
    assert archive_status == "FAILED"

    result = archive.archive_batch(delivery.batch_id)

    assert result.archive_path.exists()
    assert sha256_file(result.archive_path) == original_checksum
    assert not delivery.source_path.exists()

    observation = inspect_recovery(
        case.database_url,
        outcome.request_id,
    )

    assert observation.action is RecoveryAction.DO_NOT_REPEAT
    assert case.publication() == previous

    # Rebuild again using durable database evidence after source removal.
    with psycopg.connect(
        case.database_url,
        row_factory=dict_row,
    ) as connection:
        lock_financial_state(connection)

        frozen_time = connection.execute(
            "SELECT clock_timestamp() AS value"
        ).fetchone()["value"]

        enqueue_full_rebuild(
            connection,
            trigger_receipt_id=delivery.receipt_ids[0],
            canonical_changed=True,
            affects_published_history=True,
            change_key=f"post-archive-verification:{delivery.batch_id}",
            reason_code="SCENARIO_VERIFICATION",
            as_of_utc=frozen_time,
        )

    rebuilt = case.recover_next()

    assert rebuilt.logical_fingerprint == outcome.logical_fingerprint


@pytest.mark.parametrize(
    "point",
    [
        "after_data_fsync",
        "after_data_rename",
        "after_manifest_fsync",
    ],
)
def test_scenario_partial_delivery_never_commits(
    recovery_scenario,
    point,
):
    case = recovery_scenario
    path = case.inbox_root / "unfinished-payments.json"

    def fail(current_point):
        if current_point == point:
            raise RuntimeError("Producer stopped")

    with pytest.raises(RuntimeError, match="Producer stopped"):
        publish_source_file(
            path,
            records=[scenario_payment()],
            source_system=REPORT_SOURCES[ReportType.PAYMENTS].value,
            report_type=ReportType.PAYMENTS.name,
            report_date=SCENARIO_REPORT_DATE,
            entity_type=EntityType.PAYMENT.name,
            failure_hook=fail,
        )

    calls = []

    def ingest_if_ready(ready):
        calls.append(ready)

        # If the gate incorrectly accepts this file, the fixture
        # would create committed evidence and this test would fail.
        case.deliver(
            EntityType.PAYMENT,
            list(ready.records),
            filename="incorrectly-accepted.json",
        )

    result = ingest_ready_source(
        path,
        ingest=ingest_if_ready,
    )

    assert result.state is FileGateState.WAITING
    assert calls == []

    with psycopg.connect(case.database_url) as connection:
        for table in (
            "ingestion_batch",
            "source_receipt",
            "recovery_request",
        ):
            # table names are fixed test constants.
            count = connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

            assert count == 0

    assert case.publication() is None


def test_scenario_quarantined_amount_cannot_change_money(
    recovery_scenario,
):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    baseline = case.recover_next()

    malformed = scenario_payment()
    malformed["payment_id"] = "PAY-MALFORMED-001"
    malformed["amount"] = "one hundred"

    case.deliver(EntityType.PAYMENT, [malformed])
    recovered = case.recover_next()

    assert recovered.logical_fingerprint == baseline.logical_fingerprint

    scenario_assert_money(
        case.publication(),
        captured="100.00",
        fees="2.00",
        refunded="0.00",
    )

    rejected = case.publication()["result_payload"]["quarantine_records"]

    assert len(rejected) == 1
    assert rejected[0]["source_id"] == "PAY-MALFORMED-001"
    assert rejected[0]["reason_code"] == "INVALID_AMOUNT"

    case.assert_evidence_conserved()


def test_scenario_missing_manifest_evidence_never_becomes_complete(
    recovery_scenario,
):
    case = recovery_scenario

    case.deliver(EntityType.PAYMENT, [scenario_payment()])
    case.recover_next()

    document = case.publication()["result_payload"]

    # Ensure this checks a nonempty financial calculation.
    assert document["expected_movements"]

    reports = document["source_completeness"]

    assert len(reports) == 6
    assert all(
        report["state"] != SourceCompleteness.COMPLETE.value
        for report in reports
    )