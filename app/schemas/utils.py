from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional, List, Tuple


class Msg(BaseModel):
    status: str
    message: Optional[str]


class Item(BaseModel):
    tag: str
    value: Optional[float] = 0


class ItemWithPercent(Item):
    percent: Optional[float] = 0


class ItemWithMax(BaseModel):
    tag: str
    value: Optional[float] = 0
    max: Optional[float] = 0
    percent: Optional[float] = 0


class ItemWithTimestamp(Item):
    timestamp: Optional[datetime] = None


class TimeseriesItem(BaseModel):
    tag: str
    values: List[Tuple[datetime, float]] = []


class DateseriesItem(BaseModel):
    tag: str
    values: List[Tuple[date, float]] = []


class LocationItem(BaseModel):
    tag: str
    lat: float
    lng: float


class Items(BaseModel):
    datetime: datetime
    status: str
    items: List[Item | ItemWithPercent
                | TimeseriesItem | ItemWithTimestamp | LocationItem
                | ItemWithMax | DateseriesItem] = []
