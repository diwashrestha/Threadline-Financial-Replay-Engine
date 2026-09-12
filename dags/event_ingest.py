"""File-based ingest into raw.events (no Airflow dependency — unit-test friendly)."""
import json
import logging
import os
import shutil
from typing import Callable, Optional

from event_validation import _validate_event

log = logging.getLogger(__name__)

RAW_DIR = '/data/events/raw'
BAD_DIR = '/data/events/bad'
PROCESSED_DIR = '/data/events/processed'

# Idempotent load: duplicate source_file is ignored.
INSERT_SQL = """
    INSERT INTO raw.events (event_type, user_id, product_id, event_timestamp, source_file)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (source_file) DO NOTHING;
"""


def _move_file(src_path: str, dest_dir: str) -> None:
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, os.path.basename(src_path))
    shutil.move(src_path, dest_path)
    log.info('Moved %s to %s', src_path, dest_path)


def list_pending_json_files(raw_dir: str = RAW_DIR) -> list[str]:
    if not os.path.isdir(raw_dir):
        return []
    return [f for f in os.listdir(raw_dir) if f.endswith('.json')]


def has_pending_json_files(raw_dir: str = RAW_DIR) -> bool:
    pending = list_pending_json_files(raw_dir)
    if pending:
        log.info('Found %s pending file(s) in raw/', len(pending))
    return bool(pending)


def run_ingest(
    raw_dir: str = RAW_DIR,
    bad_dir: str = BAD_DIR,
    processed_dir: str = PROCESSED_DIR,
    hook=None,
    hook_factory: Optional[Callable[[], object]] = None,
) -> int:
    """
    Validate JSON files, bulk-insert into Postgres, move valid files to processed/.
    Returns the number of rows in the INSERT batch (not necessarily new DB rows).
    """
    pending = list_pending_json_files(raw_dir)
    if not pending:
        log.info('No pending .json files in raw/')
        return 0

    records = []
    files_to_process = []

    for filename in pending:
        filepath = os.path.join(raw_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning('Error reading %s: %s', filepath, e)
            _move_file(filepath, bad_dir)
            continue

        event = _validate_event(data, filename)
        if event is None:
            _move_file(filepath, bad_dir)
            continue

        records.append((
            event['event_type'],
            event['user_id'],
            event['product_id'],
            event['event_timestamp'],
            filename,
        ))
        files_to_process.append(filename)

    if not records:
        log.info('No valid files to load after validation')
        return 0

    if hook is None:
        if hook_factory is not None:
            hook = hook_factory()
        else:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            hook = PostgresHook(postgres_conn_id='postgres_default')

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, records)
        conn.commit()
    finally:
        conn.close()

    log.info('Loaded %s event(s) into raw.events', len(records))

    for filename in files_to_process:
        filepath = os.path.join(raw_dir, filename)
        if os.path.exists(filepath):
            _move_file(filepath, processed_dir)

    return len(records)
