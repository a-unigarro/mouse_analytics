import os
from fastapi import APIRouter
from sqlalchemy import func
from database.database import SessionLocal
from database.models import HeatmapCell


router = APIRouter()

# Keep in sync with HEATMAP_GRID_SIZE used by analytics-consumer, so the
# frontend knows how large (in px) each returned grid cell actually is.
HEATMAP_GRID_SIZE = int(os.getenv("HEATMAP_GRID_SIZE", "50"))


@router.get("/heatmap")
def get_heatmap(event_type: str = "click"):
    """Aggregated heatmap cell counts, summed across all windows, for the
    given event_type ('click' or 'mousemove')."""
    with SessionLocal() as session:
        rows = (
            session.query(
                HeatmapCell.grid_x,
                HeatmapCell.grid_y,
                func.sum(HeatmapCell.count).label("count"),
            )
            .filter(HeatmapCell.event_type == event_type)
            .group_by(HeatmapCell.grid_x, HeatmapCell.grid_y)
            .all()
        )

    return {
        "grid_size": HEATMAP_GRID_SIZE,
        "event_type": event_type,
        "cells": [
            {"grid_x": r.grid_x, "grid_y": r.grid_y, "count": r.count}
            for r in rows
        ],
    }