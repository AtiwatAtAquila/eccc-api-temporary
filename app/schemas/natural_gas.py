from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime


class TankTableCreate(BaseModel):
    level_cm: int
    lmpt2_tank1_m3: Optional[float] = None
    lmpt2_tank2_m3: Optional[float] = None


class EodValueCreate(BaseModel):
    date: date
    tag: str
    value: float
    update_timestamp: datetime
