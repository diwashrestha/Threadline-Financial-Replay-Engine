"""Event JSON validation (no Airflow dependency — unit-test friendly)."""
from datetime import datetime
from typing import Any, Dict, Optional
import logging

log = logging.getLogger(__name__)

# Required keys in incoming JSON files (product_id may be null).
EXPECTED_FIELDS = {'event_type', 'user_id', 'product_id', 'timestamp'}
VALID_EVENT_TYPES = frozenset({'page_view', 'add_to_cart', 'purchase'})


def _parse_event_timestamp(value: str) -> Optional[datetime]:
    """Parse ISO-8601; store naive UTC for Postgres TIMESTAMP."""
    try:
        normalized = value.replace('Z', '+00:00')
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)
        return parsed
    except (ValueError, TypeError):
        return None


def _validate_event(data: dict, filename: str) -> Optional[Dict[str, Any]]:
    """Return normalized event dict, or None if the file should go to bad/."""
    if not EXPECTED_FIELDS.issubset(data.keys()):
        missing = EXPECTED_FIELDS - data.keys()
        log.warning('File %s missing required fields: %s', filename, sorted(missing))
        return None

    if data['event_type'] not in VALID_EVENT_TYPES:
        log.warning('File %s has invalid event_type: %r', filename, data['event_type'])
        return None

    if not isinstance(data['user_id'], int) or data['user_id'] <= 0:
        log.warning('File %s contain bad user', filename)
        return None

    if not isinstance(data.get('product_id'), (int, type(None))):
        log.warning('File %s contain bad product id', filename)
        return None

    if not isinstance(data['timestamp'], str):
        log.warning('File %s has non-string timestamp', filename)
        return None

    event_timestamp = _parse_event_timestamp(data['timestamp'])
    if event_timestamp is None:
        log.warning('File %s has unparseable timestamp: %r', filename, data['timestamp'])
        return None

    return {
        'event_type': data['event_type'],
        'user_id': data['user_id'],
        'product_id': data.get('product_id'),
        'event_timestamp': event_timestamp,
    }
