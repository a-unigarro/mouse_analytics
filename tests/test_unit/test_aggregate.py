from consumers.analytics_consumer import get_heatmap_cell, create_session_aggregates, create_heatmap_aggregates


def test_heatmap_cell_calculation():
    grid_x, grid_y = get_heatmap_cell(
        180,
        50,
        50,
    )
    assert grid_x == 3
    assert grid_y == 1

def test_session_click_aggregation():
    counts = {
        ("session-123", "click"): 5,
    }

    aggregates = create_session_aggregates(
        counts=counts,
        window_start_iso="2026-09-07T10:00:00+00:00",
        window_end_iso="2026-09-07T10:00:10+00:00",
        duration=10,
    )

    assert len(aggregates) == 1

    aggregate = aggregates[0]

    assert aggregate.session_id == "session-123"
    assert aggregate.event_type == "click"
    assert aggregate.count == 5
    assert aggregate.rate_per_second == 0.5



def test_session_click_aggregation_multiple_sessions():
    counts = {
        ("session-123", "click"): 5,
        ("session-456", "click"): 10,
    }

    aggregates = create_session_aggregates(
        counts=counts,
        window_start_iso="2026-09-07T10:00:00+00:00",
        window_end_iso="2026-09-07T10:00:10+00:00",
        duration=10,
    )
    print(counts.items())

    assert len(aggregates) == 2

    assert aggregates[0].count == 5
    assert aggregates[1].count == 10


def test_session_click_aggregation_zero_duration():
    counts = {
        ("session-123", "click"): 5,
    }

    aggregates = create_session_aggregates(
        counts=counts,
        window_start_iso="2026-09-07T10:00:00+00:00",
        window_end_iso="2026-09-07T10:00:00+00:00",
        duration=0,
    )

    aggregate = aggregates[0]

    assert aggregate.count == 5
    assert aggregate.rate_per_second == 0





def test_heatmap_aggregation():
    counts = {
        ("session-123", "click", 3, 1, "button"): 4,
    }

    aggregates = create_heatmap_aggregates(
        counts=counts,
        window_start_iso="2026-09-07T10:00:00+00:00",
        window_end_iso="2026-09-07T10:00:10+00:00",
    )

    assert len(aggregates) == 1

    aggregate = aggregates[0]

    assert aggregate.session_id == "session-123"
    assert aggregate.event_type == "click"
    assert aggregate.grid_x == 3
    assert aggregate.grid_y == 1
    assert aggregate.element == "button"
    assert aggregate.count == 4

def test_heatmap_aggregation_multiple_cells():
    counts = {
        ("session-123", "click", 3, 1, "button"): 4,
        ("session-123", "click", 4, 1, "button"): 2,
    }

    aggregates = create_heatmap_aggregates(
        counts=counts,
        window_start_iso="2026-09-07T10:00:00+00:00",
        window_end_iso="2026-09-07T10:00:10+00:00",
    )

    assert len(aggregates) == 2

    assert aggregates[0].grid_x == 3
    assert aggregates[0].count == 4

    assert aggregates[1].grid_x == 4
    assert aggregates[1].count == 2