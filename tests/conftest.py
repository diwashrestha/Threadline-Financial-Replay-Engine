"""Shared pytest fixtures and PYTHONPATH for dags/ modules."""
import os
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = PROJECT_ROOT / 'dags'
for path in (str(DAGS_DIR), str(PROJECT_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture
def temp_events_dir():
    """Temporary raw / bad / processed directories (auto-deleted after test)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        raw_dir = os.path.join(tmpdir, 'raw')
        bad_dir = os.path.join(tmpdir, 'bad')
        processed_dir = os.path.join(tmpdir, 'processed')
        os.makedirs(raw_dir)
        os.makedirs(bad_dir)
        os.makedirs(processed_dir)
        yield {
            'root': tmpdir,
            'raw': raw_dir,
            'bad': bad_dir,
            'processed': processed_dir,
        }


@pytest.fixture
def sample_valid_json():
    return {
        'event_type': 'page_view',
        'user_id': 123,
        'product_id': 456,
        'timestamp': '2025-05-22T10:00:00Z',
    }


@pytest.fixture
def sample_invalid_missing_event_type():
    return {
        'user_id': 123,
        'product_id': 456,
        'timestamp': '2025-05-22T10:00:00Z',
    }


@pytest.fixture
def sample_invalid_missing_user_id():
    return {
        'event_type': 'page_view',
        'product_id': 456,
        'timestamp': '2025-05-22T10:00:00Z',
    }


@pytest.fixture
def sample_invalid_missing_timestamp():
    return {
        'event_type': 'page_view',
        'user_id': 123,
        'product_id': 456,
    }
