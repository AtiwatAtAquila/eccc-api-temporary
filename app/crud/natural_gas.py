import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from app.models.natural_gas import TankTable, EodValue
from app.schemas.natural_gas import TankTableCreate, EodValueCreate
from math import ceil, floor
from datetime import date, timedelta
from collections import defaultdict
from fastapi import HTTPException

logger = logging.getLogger(__name__)


async def get_eod_value(db: AsyncSession, date_from: date, date_to: date,
                        tag: str):
    valid_tags = ['invent', 'sendout']
    if tag not in valid_tags:
        logger.error(
            f'{tag} is not valid. The avialable tags are {valid_tags}')
        raise HTTPException(status_code=500, detail='internal error')

    stmt = select(EodValue).where(
        EodValue.tag.in_([f"lmpt1_{tag}", f"lmpt2_{tag}"]), EodValue.date
        >= date_from, EodValue.date <= date_to)
    result = await db.execute(stmt)
    records = result.scalars().all()

    # Group by (tag, date) for quick access
    value_map = defaultdict(float)
    for record in records:
        key = (record.tag, record.date)
        value_map[key] = record.value

    # Yield values per day
    date_result = date_from
    while date_result <= date_to:
        lmpt1_value = value_map.get((f"lmpt1_{tag}", date_result), 0)
        lmpt2_value = value_map.get((f"lmpt2_{tag}", date_result), 0)

        yield {
            "date": date_result,
            "total": lmpt1_value + lmpt2_value,
            "lmpt1": lmpt1_value,
            "lmpt2": lmpt2_value,
        }
        date_result += timedelta(days=1)


async def upsert_tank_table(db: AsyncSession, item: TankTableCreate):
    stmt = select(TankTable).where(TankTable.level_cm == item.level_cm, )
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        # Update the existing record
        await db.execute(
            update(TankTable).where(
                TankTable.level_cm == existing.level_cm).values(
                    lmpt2_tank1_m3=item.lmpt2_tank1_m3,
                    lmpt2_tank2_m3=item.lmpt2_tank2_m3))
    else:
        # Insert new record
        new_record = TankTable(level_cm=item.level_cm,
                               lmpt2_tank1_m3=item.lmpt2_tank1_m3,
                               lmpt2_tank2_m3=item.lmpt2_tank2_m3)
        db.add(new_record)

    await db.commit()


async def upsert_eod_value(db: AsyncSession, item: EodValueCreate):
    stmt = select(EodValue).where(EodValue.date == item.date,
                                  EodValue.tag == item.tag)
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        # Update the existing record
        await db.execute(
            update(EodValue).where(EodValue.id == existing.id).values(
                value=item.value, update_timestamp=item.update_timestamp))
    else:
        # Insert new record
        new_record = EodValue(tag=item.tag,
                              value=item.value,
                              update_timestamp=item.update_timestamp,
                              date=item.date)
        db.add(new_record)

    await db.commit()


async def cal_inventory(db: AsyncSession, level: float, tank: int) -> float:
    level_cm = level / 10
    level_cm_ceil = ceil(level_cm)
    level_cm_floor = floor(level_cm)

    # Case: exact match
    if level_cm_ceil == level_cm_floor:
        stmt = select(TankTable).where(TankTable.level_cm == level_cm)
        result = await db.execute(stmt)
        record = result.scalar_one_or_none()
        if not record:
            return 0  # or raise exception

        return record.lmpt2_tank1_m3 if tank == 1 else record.lmpt2_tank2_m3

    # Case: need interpolation
    stmt_ceil = select(TankTable).where(TankTable.level_cm == level_cm_ceil)
    stmt_floor = select(TankTable).where(TankTable.level_cm == level_cm_floor)

    result_ceil = await db.execute(stmt_ceil)
    record_ceil = result_ceil.scalar_one_or_none()

    result_floor = await db.execute(stmt_floor)
    record_floor = result_floor.scalar_one_or_none()

    if not record_ceil or not record_floor:
        return 0  # or raise exception

    invent_ceil = record_ceil.lmpt2_tank1_m3 if tank == 1 else record_ceil.lmpt2_tank2_m3
    invent_floor = record_floor.lmpt2_tank1_m3 if tank == 1 else record_floor.lmpt2_tank2_m3

    invent_diff = (level_cm - level_cm_floor) / (
        level_cm_ceil - level_cm_floor) * (invent_ceil - invent_floor)
    return invent_floor + invent_diff
