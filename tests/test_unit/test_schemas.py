from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from shared.schemas import UserEvent


def test_valid_user_event():
    iso_now = datetime.now(timezone.utc).isoformat()

    event_data = {
        "session_id": "session-123",
        "event_type": "click",
        "user_id": "anonymous",
        "timestamp": iso_now,
        "x": 180,
        "y": 50,
    }

    event = UserEvent(**event_data)

    assert event.session_id == "session-123"
    assert event.event_type == "click"
    assert event.user_id == "anonymous"
    assert event.timestamp == datetime.fromisoformat(iso_now)
    assert event.x == 180
    assert event.y == 50
    assert event.page is None
    assert event.element is None

def test_user_event_invalid_timestamp():
    event_data = {
        "session_id": "session-123",
        "event_type": "click",
        "user_id": "anonymous",
        "timestamp": "this-is-not-a-timestamp",
        "x": 180,
        "y": 50,
    }

    with pytest.raises(ValidationError):
        UserEvent(**event_data)

def test_user_event_missing_session_id():
    iso_now = datetime.now(timezone.utc).isoformat()

    event_data = {
        "event_type": "click",
        "user_id": "anonymous",
        "timestamp": iso_now,
        "x": 180,
        "y": 50,
    }

    with pytest.raises(ValidationError):
        UserEvent(**event_data)


