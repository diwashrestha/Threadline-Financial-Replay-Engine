"""Unit tests for event_validation and event_ingest."""
import json
import os
from unittest.mock import MagicMock

import pytest

from event_ingest import has_pending_json_files, list_pending_json_files, run_ingest
from event_validation import VALID_EVENT_TYPES, _parse_event_timestamp, _validate_event


class TestValidateEvent:
    def test_valid_event(self, sample_valid_json):
        result = _validate_event(sample_valid_json, 'valid_event.json')
        assert result is not None
        assert result['event_type'] == 'page_view'
        assert result['user_id'] == 123
        assert result['product_id'] == 456
        assert result['event_timestamp'].year == 2025

    @pytest.mark.parametrize('event_type', sorted(VALID_EVENT_TYPES))
    def test_valid_event_types(self, sample_valid_json, event_type):
        event = sample_valid_json.copy()
        event['event_type'] = event_type
        result = _validate_event(event, f'{event_type}.json')
        assert result is not None
        assert result['event_type'] == event_type

    def test_missing_event_type_returns_none(self, sample_invalid_missing_event_type):
        result = _validate_event(sample_invalid_missing_event_type, 'bad_no_event_type.json')
        assert result is None

    def test_missing_user_id_returns_none(self, sample_invalid_missing_user_id):
        result = _validate_event(sample_invalid_missing_user_id, 'bad_no_user.json')
        assert result is None

    def test_missing_timestamp_returns_none(self, sample_invalid_missing_timestamp):
        result = _validate_event(sample_invalid_missing_timestamp, 'bad_no_ts.json')
        assert result is None

    def test_missing_product_id_key_returns_none(self, sample_valid_json):
        event = sample_valid_json.copy()
        del event['product_id']
        result = _validate_event(event, 'bad_no_product_key.json')
        assert result is None

    def test_valid_event_with_null_product_id(self, sample_valid_json):
        event = sample_valid_json.copy()
        event['product_id'] = None
        result = _validate_event(event, 'valid_null_product.json')
        assert result is not None
        assert result['product_id'] is None

    def test_invalid_user_id_type(self, sample_valid_json):
        event = sample_valid_json.copy()
        event['user_id'] = 'not_an_int'
        assert _validate_event(event, 'bad_user_id_str.json') is None

    @pytest.mark.parametrize('bad_user_id', [0, -1])
    def test_invalid_user_id_not_positive(self, sample_valid_json, bad_user_id):
        event = sample_valid_json.copy()
        event['user_id'] = bad_user_id
        assert _validate_event(event, f'bad_user_{bad_user_id}.json') is None

    def test_invalid_product_id_type(self, sample_valid_json):
        event = sample_valid_json.copy()
        event['product_id'] = 'not_int'
        assert _validate_event(event, 'bad_product.json') is None

    def test_timestamp_not_string(self, sample_valid_json):
        event = sample_valid_json.copy()
        event['timestamp'] = 12345
        assert _validate_event(event, 'bad_ts_type.json') is None

    def test_invalid_timestamp_format(self, sample_valid_json):
        event = sample_valid_json.copy()
        event['timestamp'] = 'not_a_date'
        assert _validate_event(event, 'bad_timestamp.json') is None

    def test_invalid_event_type(self, sample_valid_json):
        event = sample_valid_json.copy()
        event['event_type'] = 'invalid_type'
        assert _validate_event(event, 'bad_event_type.json') is None

    @pytest.mark.parametrize(
        'timestamp,expected_year',
        [
            ('2025-05-22T10:00:00Z', 2025),
            ('2025-05-22T10:00:00+00:00', 2025),
        ],
    )
    def test_parse_event_timestamp_iso_formats(self, timestamp, expected_year):
        parsed = _parse_event_timestamp(timestamp)
        assert parsed is not None
        assert parsed.year == expected_year
        assert parsed.tzinfo is None

    def test_parse_event_timestamp_invalid(self):
        assert _parse_event_timestamp('not-a-date') is None


class TestRunIngest:
    def test_ingest_happy_path(self, temp_events_dir, sample_valid_json):
        raw_dir = temp_events_dir['raw']
        bad_dir = temp_events_dir['bad']
        processed_dir = temp_events_dir['processed']

        with open(os.path.join(raw_dir, 'valid.json'), 'w', encoding='utf-8') as f:
            json.dump(sample_valid_json, f)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        loaded = run_ingest(
            raw_dir=raw_dir,
            bad_dir=bad_dir,
            processed_dir=processed_dir,
            hook=mock_hook,
        )

        assert loaded == 1
        mock_cursor.executemany.assert_called_once()
        _, records = mock_cursor.executemany.call_args[0]
        assert len(records) == 1
        assert records[0][0] == sample_valid_json['event_type']
        assert records[0][4] == 'valid.json'
        assert not os.path.exists(os.path.join(raw_dir, 'valid.json'))
        assert os.path.exists(os.path.join(processed_dir, 'valid.json'))

    def test_ingest_invalid_json_moves_to_bad(self, temp_events_dir):
        raw_dir = temp_events_dir['raw']
        bad_dir = temp_events_dir['bad']
        processed_dir = temp_events_dir['processed']

        bad_file = os.path.join(raw_dir, 'broken.json')
        with open(bad_file, 'w', encoding='utf-8') as f:
            f.write('{ not json')

        loaded = run_ingest(
            raw_dir=raw_dir,
            bad_dir=bad_dir,
            processed_dir=processed_dir,
            hook=MagicMock(),
        )

        assert loaded == 0
        assert os.path.exists(os.path.join(bad_dir, 'broken.json'))
        assert not os.path.exists(bad_file)

    def test_ingest_empty_raw_dir(self, temp_events_dir):
        loaded = run_ingest(
            raw_dir=temp_events_dir['raw'],
            bad_dir=temp_events_dir['bad'],
            processed_dir=temp_events_dir['processed'],
            hook=MagicMock(),
        )
        assert loaded == 0

    def test_list_pending_when_raw_missing(self, tmp_path):
        assert list_pending_json_files(str(tmp_path / 'missing')) == []

    def test_has_pending_json_files(self, temp_events_dir, sample_valid_json):
        raw_dir = temp_events_dir['raw']
        assert has_pending_json_files(raw_dir) is False
        with open(os.path.join(raw_dir, 'one.json'), 'w', encoding='utf-8') as f:
            json.dump(sample_valid_json, f)
        assert has_pending_json_files(raw_dir) is True
