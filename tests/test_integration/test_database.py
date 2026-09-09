from database.database import SessionLocal, Base, engine
from database.models import HeatmapCell, SessionClickRate
from consumers.aggregate_writer import (
    insert_heatmap_aggregate,
    insert_session_aggregate,
)
from shared.schemas import (
    HeatmapAggregateEvent,
    SessionAggregateEvent,
)


def test_insert_heatmap_aggregate_integration():
    Base.metadata.create_all(bind=engine)

    event = HeatmapAggregateEvent(
        session_id="integration-test-session",
        metric="heatmap_cell",
        window_start="2026-09-07T10:00:00+00:00",
        window_end="2026-09-07T10:00:10+00:00",
        event_type="mousemove",
        grid_x=3,
        grid_y=2,
        element="button",
        count=8,
    )

    insert_heatmap_aggregate(event)

    with SessionLocal() as session:
        row = (
            session.query(HeatmapCell)
            .filter_by(session_id="integration-test-session")
            .first()
        )

    assert row is not None
    assert row.grid_x == 3
    assert row.grid_y == 2
    assert row.element == "button"
    assert row.count == 8


def test_insert_session_aggregate_integration():
    Base.metadata.create_all(bind=engine)

    event = SessionAggregateEvent(
        session_id="integration-session-2",
        metric="session_event_count",
        window_start="2026-09-07T10:00:00+00:00",
        window_end="2026-09-07T10:00:10+00:00",
        event_type="mousemove",
        count=80,
        rate_per_second=8.0,
    )

    insert_session_aggregate(event)

    with SessionLocal() as session:
        row = (
            session.query(SessionClickRate)
            .filter_by(session_id="integration-session-2")
            .first()
        )

    assert row is not None
    assert row.session_id == "integration-session-2"
    assert row.event_type == "mousemove"
    assert row.count == 80
    assert row.rate_per_second == 8.0
