from unittest.mock import patch
from consumers.aggregate_writer import handle_message, insert_session_aggregate, insert_heatmap_aggregate
from shared.schemas import (
    HeatmapAggregateEvent,
    SessionAggregateEvent,
)

def test_handle_session_aggregate():
    data = {
        "session_id": "session-123",
        "metric": "session_event_count",
        "window_start": "2026-09-07T10:00:00+00:00",
        "window_end": "2026-09-07T10:00:10+00:00",
        "event_type": "click",
        "count": 5,
        "rate_per_second": 0.5,
    }

    with patch(
        "consumers.aggregate_writer.insert_session_aggregate"
    ) as mock_insert:
        handle_message(data)
        mock_insert.assert_called_once()
        event = mock_insert.call_args.args[0]

        assert event.session_id == "session-123"
        assert event.metric == "session_event_count"
        assert event.event_type == "click"
        assert event.count == 5
        assert event.rate_per_second == 0.5


def test_handle_heatmap_aggregate():
    data = {
        "session_id": "session-123",
        "metric": "heatmap_cell",
        "window_start": "2026-09-07T10:00:00+00:00",
        "window_end": "2026-09-07T10:00:10+00:00",
        "event_type": "mousemove",
        "grid_x": 3,
        "grid_y": 2,
        "element": "button",
        "count": 8,
    }

    with patch(
        "consumers.aggregate_writer.insert_heatmap_aggregate"
    ) as mock_insert:

        handle_message(data)

        mock_insert.assert_called_once()

        event = mock_insert.call_args.args[0]

        assert event.session_id == "session-123"
        assert event.metric == "heatmap_cell"
        assert event.event_type == "mousemove"
        assert event.grid_x == 3
        assert event.grid_y == 2
        assert event.element == "button"
        assert event.count == 8


def test_handle_unknown_metric():
    data = {
        "session_id": "session-123",
        "metric": "something_unknown",
    }

    with patch(
        "consumers.aggregate_writer.insert_session_aggregate"
    ) as mock_session_insert, patch(
        "consumers.aggregate_writer.insert_heatmap_aggregate"
    ) as mock_heatmap_insert:

        handle_message(data)

        mock_session_insert.assert_not_called()
        mock_heatmap_insert.assert_not_called()





def test_insert_session_aggregate():
    event = SessionAggregateEvent(
        session_id="session-123",
        metric="session_event_count",
        window_start="2026-09-07T10:00:00+00:00",
        window_end="2026-09-07T10:00:10+00:00",
        event_type="click",
        count=5,
        rate_per_second=0.5,
    )

    with patch(
        "consumers.aggregate_writer.SessionLocal"
    ) as mock_session_local:

        mock_session = mock_session_local.return_value.__enter__.return_value

        insert_session_aggregate(event)

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        row = mock_session.add.call_args.args[0]

        assert row.session_id == "session-123"
        assert row.window_start == event.window_start
        assert row.window_end == event.window_end
        assert row.event_type == "click"
        assert row.count == 5
        assert row.rate_per_second == 0.5

def test_insert_heatmap_aggregate():
    event = HeatmapAggregateEvent(
        session_id="session-123",
        metric="heatmap_cell",
        window_start="2026-09-07T10:00:00+00:00",
        window_end="2026-09-07T10:00:10+00:00",
        event_type="mousemove",
        grid_x=3,
        grid_y=2,
        element="button",
        count=8,
    )

    with patch(
        "consumers.aggregate_writer.SessionLocal"
    ) as mock_session_local:

        mock_session = (
            mock_session_local
            .return_value
            .__enter__
            .return_value
        )

        insert_heatmap_aggregate(event)

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

        row = mock_session.add.call_args.args[0]

        assert row.session_id == "session-123"
        assert row.window_start == event.window_start
        assert row.window_end == event.window_end
        assert row.event_type == "mousemove"
        assert row.grid_x == 3
        assert row.grid_y == 2
        assert row.element == "button"
        assert row.count == 8