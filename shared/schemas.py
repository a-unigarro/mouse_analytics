from datetime import datetime
from pydantic import BaseModel, Field


from datetime import datetime
from pydantic import BaseModel


class UserEvent(BaseModel):
    session_id: str
    event_type: str
    user_id: str
    timestamp: datetime

    x: int 
    y: int 
    
    page: str | None = None
    element: str | None = None


# ============================================================
# Esquema Base de Agregaciones (Campos comunes)
# ============================================================
class BaseAggregateEvent(BaseModel):
    session_id: str | None = None
    metric: str
    window_start: datetime
    window_end: datetime
    event_type: str | None = None
    count: int

class SessionAggregateEvent(BaseAggregateEvent):        
    rate_per_second: float | None = None


class HeatmapAggregateEvent(BaseAggregateEvent):    
    grid_x: int 
    grid_y: int 
    element: str | None = None


    