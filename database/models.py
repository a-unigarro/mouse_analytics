from sqlalchemy import Column, Integer, String, Float, DateTime
from database.database import Base
 
 
class SessionClickRate(Base):
    __tablename__ = "session_click_rates"
 
    id = Column(Integer, primary_key=True)
    session_id = Column(String, nullable=False)
    window_start = Column(DateTime, nullable=False)
    window_end = Column(DateTime, nullable=False)
    event_type = Column(String)
    count = Column(Integer, nullable=False)
    rate_per_second = Column(Float)
 
 
class HeatmapCell(Base):
    __tablename__ = "heatmap_cells"
 
    id = Column(Integer, primary_key=True)
    session_id = Column(String, nullable=False)
    window_start = Column(DateTime, nullable=False)
    window_end = Column(DateTime, nullable=False)
    event_type = Column(String, nullable=False)
    grid_x = Column(Integer, nullable=False)
    grid_y = Column(Integer, nullable=False)
    count = Column(Integer, nullable=False)
 