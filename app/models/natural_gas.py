from sqlalchemy import Column, Integer, Float, Date, DateTime, String
from app.db.base import Base


class TankTable(Base):
    __tablename__ = 'naturalGasTanktable'

    level_cm = Column(Integer, primary_key=True, index=True)
    lmpt2_tank1_m3 = Column(Float, nullable=True)
    lmpt2_tank2_m3 = Column(Float, nullable=True)


class EodValue(Base):
    __tablename__ = 'naturalGasEodValue'

    id = Column(Integer,
                unique=True,
                primary_key=True,
                index=True,
                autoincrement=True,
                nullable=False)
    date = Column(Date, nullable=False, index=True)
    tag = Column(String(32), nullable=False, index=True)
    value = Column(Float, nullable=True)
    update_timestamp = Column(DateTime, nullable=False)
