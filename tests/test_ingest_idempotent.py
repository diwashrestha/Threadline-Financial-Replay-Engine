"""Idempotency: same source_file must not create duplicate logical rows (ON CONFLICT)."""
import json
import os
import shutil

import pytest

from event_ingest import INSERT_SQL, run_ingest


class _FakeCursor:
    def __init__(self, store: dict):
        self._store = store

    def executemany(self, sql, records):
        assert 'ON CONFLICT' in sql
        assert 'source_file' in sql
        for record in records:
            source_file = record[4]
            if source_file not in self._store:
                self._store[source_file] = record

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeConnection:
    def __init__(self, store: dict):
        self._store = store
        self.committed = False

    def cursor(self):
        return _FakeCursor(self._store)

    def commit(self):
        self.committed = True

    def close(self):
        pass


class _FakeHook:
    """Simulates Postgres ON CONFLICT DO NOTHING per source_file."""

    def __init__(self):
        self.store: dict = {}

    def get_conn(self):
        return _FakeConnection(self.store)

    @property
    def row_count(self):
        return len(self.store)


@pytest.fixture
def idempotent_hook():
    return _FakeHook()


class TestIngestIdempotent:
    def test_insert_sql_declares_on_conflict_for_source_file(self):
        assert 'ON CONFLICT' in INSERT_SQL
        assert 'source_file' in INSERT_SQL
        assert 'DO NOTHING' in INSERT_SQL

    def test_reprocessing_same_file_does_not_duplicate_rows(
        self, temp_events_dir, sample_valid_json, idempotent_hook,
    ):
        raw_dir = temp_events_dir['raw']
        bad_dir = temp_events_dir['bad']
        processed_dir = temp_events_dir['processed']
        src_name = 'event_idempotent.json'

        with open(os.path.join(raw_dir, src_name), 'w', encoding='utf-8') as f:
            json.dump(sample_valid_json, f)

        loaded_first = run_ingest(
            raw_dir=raw_dir,
            bad_dir=bad_dir,
            processed_dir=processed_dir,
            hook=idempotent_hook,
        )
        assert loaded_first == 1
        assert idempotent_hook.row_count == 1
        assert not os.path.exists(os.path.join(raw_dir, src_name))
        assert os.path.exists(os.path.join(processed_dir, src_name))

        # Re-drive: same file back in raw/ must not create a second logical row
        shutil.copy(
            os.path.join(processed_dir, src_name),
            os.path.join(raw_dir, src_name),
        )

        loaded_second = run_ingest(
            raw_dir=raw_dir,
            bad_dir=bad_dir,
            processed_dir=processed_dir,
            hook=idempotent_hook,
        )
        assert loaded_second == 1
        assert idempotent_hook.row_count == 1
