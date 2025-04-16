import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select, update, or_, and_
from app.models.electric import DummyData, Project, PeakDay
from app.schemas.electric import DummyDataCreate, ProjectCreate, PeakDayCreate
from typing import AsyncGenerator
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)


async def create_dummy_data(db: AsyncSession,
                            data: DummyDataCreate) -> DummyData:
    new_data = DummyData(**data.dict())
    db.add(new_data)
    await db.commit()
    await db.refresh(new_data)
    return new_data


async def get_latest_dummy_data(
        db: AsyncSession, category: str,
        data_timestamp: datetime) -> AsyncGenerator[dict, None]:
    # *Normalize to start of the month
    data_timestamp = data_timestamp.replace(day=1, second=0, microsecond=0)

    # *Base filters
    base_filters = [
        DummyData.category == category,
        DummyData.data_timestamp == data_timestamp,
    ]

    # *Get latest submit_timestamp for the given data_timestamp
    latest_submit_stmt = select(func.max(
        DummyData.submit_timestamp)).where(*base_filters)
    result = await db.execute(latest_submit_stmt)
    latest_submit_time = result.scalar()

    if not latest_submit_time:
        return  # No data available

    # *Final query: include latest submit_timestamp in filters
    final_stmt = (select(
        DummyData.data_timestamp, DummyData.submit_timestamp,
        func.sum(DummyData.value).label('value')).where(
            *base_filters,
            DummyData.submit_timestamp == latest_submit_time).group_by(
                DummyData.data_timestamp, DummyData.submit_timestamp))

    # *Stream results and yield
    stream_result = await db.stream(final_stmt)
    async for row in stream_result:
        yield {
            "data_timestamp": row.data_timestamp,
            "submit_timestamp": row.submit_timestamp,
            "value": row.value
        }


async def get_latest_dummy_data_grouped_by_zone(
        db: AsyncSession, category: str,
        data_timestamp: datetime) -> AsyncGenerator[dict, None]:
    # *Normalize to start of the month
    data_timestamp = data_timestamp.replace(day=1, second=0, microsecond=0)

    # *Base filters
    base_filters = [
        DummyData.category == category,
        DummyData.data_timestamp == data_timestamp,
    ]

    # *Get latest submit_timestamp for the given data_timestamp
    latest_submit_stmt = select(func.max(
        DummyData.submit_timestamp)).where(*base_filters)
    result = await db.execute(latest_submit_stmt)
    latest_submit_time = result.scalar()

    if not latest_submit_time:
        return  # No data available

    # *Final query: include latest submit_timestamp in filters
    final_stmt = (select(
        DummyData.data_timestamp, DummyData.submit_timestamp, DummyData.zone,
        func.sum(DummyData.value).label('value')).where(
            *base_filters,
            DummyData.submit_timestamp == latest_submit_time).group_by(
                DummyData.data_timestamp, DummyData.submit_timestamp,
                DummyData.zone))

    # *Stream results and yield
    stream_result = await db.stream(final_stmt)
    async for row in stream_result:
        yield {
            "data_timestamp": row.data_timestamp,
            "submit_timestamp": row.submit_timestamp,
            "zone": row.zone,
            "value": row.value
        }


async def get_profile_dummy_data(
        db: AsyncSession, category: str, datetime_from: datetime,
        datetime_to: datetime,
        min_interval: int) -> AsyncGenerator[dict, None]:
    current_timestamp = datetime_from.replace(second=0, microsecond=0)

    while current_timestamp <= datetime_to:
        base_filters = [
            DummyData.category == category,
            DummyData.data_timestamp == current_timestamp,
        ]

        # Get the latest submit timestamp for the current data_timestamp
        latest_submit_stmt = select(func.max(
            DummyData.submit_timestamp)).where(*base_filters)
        result = await db.execute(latest_submit_stmt)
        latest_submit_time = result.scalar()

        if latest_submit_time:
            # Query the sum of value for this timestamp and latest submission
            final_stmt = (select(
                DummyData.data_timestamp, DummyData.submit_timestamp,
                func.sum(DummyData.value).label('value')).where(
                    *base_filters,
                    DummyData.submit_timestamp == latest_submit_time).group_by(
                        DummyData.data_timestamp, DummyData.submit_timestamp))

            result = await db.execute(final_stmt)
            row = result.first()
            if row:
                yield (row.data_timestamp, row.value)

        current_timestamp += timedelta(minutes=min_interval)


async def get_profile_dummy_data_grouped_by_value_tag(
        db: AsyncSession, category: str, datetime_from: datetime,
        datetime_to: datetime,
        min_interval: int) -> AsyncGenerator[dict, None]:
    current_timestamp = datetime_from.replace(second=0, microsecond=0)

    while current_timestamp <= datetime_to:
        base_filters = [
            DummyData.category == category,
            DummyData.data_timestamp == current_timestamp,
        ]

        # Get the latest submit timestamp for the current data_timestamp
        latest_submit_stmt = select(func.max(
            DummyData.submit_timestamp)).where(*base_filters)
        result = await db.execute(latest_submit_stmt)
        latest_submit_time = result.scalar()

        if latest_submit_time:
            # Query the sum of value for this timestamp and latest submission
            final_stmt = (select(
                DummyData.data_timestamp, DummyData.submit_timestamp,
                DummyData.value_tag,
                func.sum(DummyData.value).label('value')).where(
                    *base_filters,
                    DummyData.submit_timestamp == latest_submit_time).group_by(
                        DummyData.data_timestamp, DummyData.submit_timestamp,
                        DummyData.value_tag))

            result = await db.execute(final_stmt)
            row = result.first()
            if row:
                yield row.value_tag, (row.data_timestamp, row.value)

        current_timestamp += timedelta(minutes=min_interval)


async def get_summary_peak(db: AsyncSession, peak_type: str):
    today = date.today()
    first_day_of_month = today.replace(day=1)
    first_day_of_year = today.replace(month=1, day=1)

    async def fetch_peak(start_date):
        stmt = (select(PeakDay).where(
            PeakDay.peak_type == peak_type, PeakDay.peak_date
            >= start_date).order_by(PeakDay.value.desc()).limit(1))
        result = await db.execute(stmt)
        row = result.scalar_one_or_none()
        if row:
            return {"value": row.value, "timestamp": row.peak_datetime}
        return None

    return {
        "today": await fetch_peak(today),
        "month": await fetch_peak(first_day_of_month),
        "year": await fetch_peak(first_day_of_year),
        "total": await fetch_peak(date(2000, 1, 1)),
    }


async def count_active_projects_by_fuel(db: AsyncSession,
                                        fuel: str,
                                        fuel_type: str = None) -> int:

    if fuel_type == 'renew':
        stmt = (select(func.count()).select_from(Project).where(
            or_(Project.contract_status.like('COD%'),
                Project.contract_status.like('N%')),
            and_(Project.primary_fuel_a_group_1 == 'Renewable',
                 Project.primary_fuel_a_group_3 == fuel)))
    elif fuel_type == 'fossil':
        stmt = (select(func.count()).select_from(Project).where(
            or_(Project.contract_status.like('COD%'),
                Project.contract_status.like('N%')),
            and_(Project.primary_fuel_a_group_1 == 'Fossil',
                 Project.primary_fuel_a_group_3 == fuel)))
    else:
        stmt = (select(func.count()).select_from(Project).where(
            or_(Project.contract_status.like('COD%'),
                Project.contract_status.like('N%')),
            Project.primary_fuel_a_group_3 == fuel))

    result = await db.execute(stmt)
    return result.scalar()


async def get_projects_location_by_fuel(db: AsyncSession):
    stmt = (select(Project.primary_fuel_a_group_1, Project.lat,
                   Project.lng).where(
                       and_(
                           or_(Project.contract_status.like('COD%'),
                               Project.contract_status.like('N%')),
                           Project.lat.isnot(None), Project.lng.isnot(None))))

    result = await db.execute(stmt)
    for row in result:
        yield {
            'fuel': row.primary_fuel_a_group_1,
            'lat': row.lat,
            'lng': row.lng
        }


async def upsert_project(db: AsyncSession, data: ProjectCreate):
    stmt = select(Project).where(Project.g_project_key == data.g_project_key)
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        for field, value in data.model_dump().items():
            setattr(existing, field, value)
        await db.commit()
    else:
        new_data = Project(**data.model_dump())
        db.add(new_data)
        await db.commit()
        await db.refresh(new_data)


async def upsert_peak(db: AsyncSession, peak: PeakDayCreate):
    stmt = select(PeakDay).where(PeakDay.peak_date == peak.peak_date,
                                 PeakDay.peak_type == peak.peak_type)
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        # Update the existing record
        await db.execute(
            update(PeakDay).where(PeakDay.id == existing.id).values(
                peak_datetime=peak.peak_datetime, value=peak.value))
    else:
        # Insert new record
        new_record = PeakDay(peak_date=peak.peak_date,
                             peak_datetime=peak.peak_datetime,
                             peak_type=peak.peak_type,
                             value=peak.value)
        db.add(new_record)

    await db.commit()
