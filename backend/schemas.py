from datetime import datetime
from pydantic import BaseModel, Field


class UserEvent(BaseModel):
    event_type: str
    user_id: str
    timestamp: datetime

    x: int | None = None
    y: int | None = None

    page: str | None = None

    element: str | None = None