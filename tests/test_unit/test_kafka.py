from unittest.mock import Mock

from consumers.analytics_consumer import publish_aggregate
from shared.schemas import HeatmapAggregateEvent


def test_publish_aggregate():
    producer = Mock()

    aggregate = HeatmapAggregateEvent(
        session_id="session-123",
        metric="heatmap_cell",
        window_start="2026-09-07T10:00:00+00:00",
        window_end="2026-09-07T10:00:10+00:00",
        event_type="click",
        grid_x=3,
        grid_y=1,
        element="button",
        count=4,
    )

    publish_aggregate(
        producer,
        aggregate,
        key="session-123",
    )

    producer.produce.assert_called_once()
    call_args = producer.produce.call_args
    assert call_args.args[0] == "mouse-events-aggregated"
    assert call_args.kwargs["key"] == "session-123"
#    print(call_args)
    producer.poll.assert_called_once_with(0)