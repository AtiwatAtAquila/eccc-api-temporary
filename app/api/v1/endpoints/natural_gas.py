import logging
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, date, timedelta, time
from fastapi import APIRouter, UploadFile, File, Depends
from app.crud import natural_gas as crud
from app.crud import tso_api, pttlng_api
from app.db.session import get_db
from app.schemas import natural_gas as schemas
from app.schemas.utils import Items, ItemWithPercent, ItemWithMax, Msg, DateseriesItem
from time import time as runtime
from fastapi import HTTPException
from io import StringIO
import csv

router = APIRouter()

logger = logging.getLogger(__name__)

MAX_INVENT_LMPT1 = 640000
MAX_INVENT_LMPT2 = 273311.623 + 273248.005
MAX_INVENT_GMPT = 0


@router.get("/current/supply/mmsdfd")
async def get_current_supply_mmscfd() -> Items:
    start_time = runtime()
    items = Items(datetime=datetime.now(), status='ok')
    total_item = ItemWithPercent(tag='total', percent=100)
    got_item = ItemWithPercent(tag='got')
    lng_item = ItemWithPercent(tag='lng')
    myanmar_item = ItemWithPercent(tag='myanmar')
    onshore_item = ItemWithPercent(tag='onshore')

    for record in tso_api.get_current_supply_mmscfd():
        total_item.value += record['value']
        if record['tag'] == 'got':
            got_item.value += record['value']
        elif record['tag'] == 'lng':
            lng_item.value += record['value']
        elif record['tag'] == 'myanmar':
            myanmar_item.value += record['value']
        elif record['tag'] == 'onshore':
            onshore_item.value += record['value']
    got_item.percent = got_item.value / total_item.value * 100
    lng_item.percent = lng_item.value / total_item.value * 100
    myanmar_item.percent = myanmar_item.value / total_item.value * 100
    onshore_item.percent = onshore_item.value / total_item.value * 100

    items.datetime = record['timestamp']
    items.items = [total_item, got_item, lng_item, myanmar_item, onshore_item]
    logger.info(f'runtime: {runtime() - start_time:3f} s')
    return items


@router.get("/current/demand/mmsdfd")
async def get_current_demand_mmscfd() -> Items:
    start_time = runtime()
    items = Items(datetime=datetime.now(), status='ok')
    total_item = ItemWithPercent(tag='total', percent=100)
    egat_item = ItemWithPercent(tag='egat')
    ipp_item = ItemWithPercent(tag='ipp')
    spp_item = ItemWithPercent(tag='spp')
    gsp_item = ItemWithPercent(tag='gsp')
    ind_item = ItemWithPercent(tag='ind')
    ngv_item = ItemWithPercent(tag='ngv')
    fuel_item = ItemWithPercent(tag='fuel')

    for record in tso_api.get_current_demand_mmscfd():
        total_item.value += record['value']
        if record['tag'] == 'egat':
            egat_item.value += record['value']
        elif record['tag'] == 'ipp':
            ipp_item.value += record['value']
        elif record['tag'] == 'spp':
            spp_item.value += record['value']
        elif record['tag'] == 'gsp':
            gsp_item.value += record['value']
        elif record['tag'] == 'ind':
            ind_item.value += record['value']
        elif record['tag'] == 'ngv':
            ngv_item.value += record['value']
        elif record['tag'] == 'fuel':
            fuel_item.value += record['value']

    egat_item.percent = egat_item.value / total_item.value * 100
    ipp_item.percent = ipp_item.value / total_item.value * 100
    spp_item.percent = spp_item.value / total_item.value * 100
    gsp_item.percent = gsp_item.value / total_item.value * 100
    ind_item.percent = ind_item.value / total_item.value * 100
    ngv_item.percent = ngv_item.value / total_item.value * 100
    fuel_item.percent = fuel_item.value / total_item.value * 100

    items.datetime = record['timestamp']
    items.items = [
        total_item, egat_item, ipp_item, spp_item, gsp_item, ind_item,
        ngv_item, fuel_item
    ]
    logger.info(f'runtime: {runtime() - start_time:3f} s')
    return items


@router.get("/current/lng/invent/m3")
async def get_current_lng_invent_m3(db: AsyncSession = Depends(
    get_db)) -> Items:
    start_time = runtime()
    items = Items(datetime=datetime.now(), status='ok')
    lmpt1_item = ItemWithMax(tag='lmpt1', max=MAX_INVENT_LMPT1)
    lmpt2_item = ItemWithMax(tag='lmpt2', value=0, max=MAX_INVENT_LMPT2)
    gmtp_item = ItemWithMax(tag='gmpt', value=0, max=MAX_INVENT_GMPT)

    data = pttlng_api.get_current_lmpt1_invent()
    items.datetime = data['timestamp']
    lmpt1_item.value = data['value']

    data = await pttlng_api.get_current_lmpt2_invent(db=db)
    if data['timestamp'] > items.datetime:
        items.datetime = data['timestamp']
    lmpt2_item.value = data['value']

    lmpt1_item.percent = lmpt1_item.value / MAX_INVENT_LMPT1 * 100
    lmpt2_item.percent = lmpt2_item.value / MAX_INVENT_LMPT2 * 100

    items.items = [lmpt1_item, lmpt2_item, gmtp_item]
    logger.info(f'runtime: {runtime() - start_time:3f} s')

    await crud.upsert_eod_value(db=db,
                                item=schemas.EodValueCreate(
                                    date=date.today(),
                                    tag='lmpt1_invent',
                                    value=lmpt1_item.value,
                                    update_timestamp=datetime.now()))
    await crud.upsert_eod_value(db=db,
                                item=schemas.EodValueCreate(
                                    date=date.today(),
                                    tag='lmpt2_invent',
                                    value=lmpt2_item.value,
                                    update_timestamp=datetime.now()))
    return items


@router.get("/eod/lng/sendout/mmscf")
async def get_eod_lng_sendout_mmscf(
        date_from: date = date.today().replace(day=1),
        date_to: date = date.today(),
        db: AsyncSession = Depends(get_db)) -> Items:
    start_time = runtime()
    items = Items(datetime=datetime.now(), status='ok')
    total_item = DateseriesItem(tag='total')
    lmpt1_item = DateseriesItem(tag='lmpt1')
    lmpt2_item = DateseriesItem(tag='lmpt2')

    async for data in crud.get_eod_value(db=db,
                                         date_from=date_from,
                                         date_to=date_to,
                                         tag='sendout'):
        total_item.values.append((data['date'], data['total']))
        lmpt1_item.values.append((data['date'], data['lmpt1']))
        lmpt2_item.values.append((data['date'], data['lmpt2']))

    items.items = [total_item, lmpt1_item, lmpt2_item]
    logger.info(f'runtime: {runtime() - start_time:3f} s')
    return items


@router.get("/eod/lng/invent/m3")
async def get_eod_lng_invent_m3(date_from: date = date.today().replace(day=1),
                                date_to: date = date.today(),
                                db: AsyncSession = Depends(get_db)) -> Items:
    start_time = runtime()
    items = Items(datetime=datetime.now(), status='ok')
    total_item = DateseriesItem(tag='total')
    lmpt1_item = DateseriesItem(tag='lmpt1')
    lmpt2_item = DateseriesItem(tag='lmpt2')

    async for data in crud.get_eod_value(db=db,
                                         date_from=date_from,
                                         date_to=date_to,
                                         tag='invent'):
        total_item.values.append((data['date'], data['total']))
        lmpt1_item.values.append((data['date'], data['lmpt1']))
        lmpt2_item.values.append((data['date'], data['lmpt2']))

    items.items = [total_item, lmpt1_item, lmpt2_item]
    logger.info(f'runtime: {runtime() - start_time:3f} s')
    return items


@router.put('/update/lng/sendout-invent')
async def update_lng_sendout_invent(start_date: date = date.today(),
                                    days_back: int = 10,
                                    db: AsyncSession = Depends(get_db)) -> Msg:
    start_time = runtime()
    msg = Msg(status='ok', message='udpate complate')

    date_process = datetime.combine(start_date, time(0, 0))
    date_process = datetime.now().replace(hour=17,
                                          minute=0,
                                          second=0,
                                          microsecond=0)
    date_process -= timedelta(days=1)
    count = days_back
    while count > 0:
        logger.info(f'processing {date_process.date().strftime("%Y-%m-%d")}')
        lmpt1_invent = 0
        try:
            for data in tso_api.get_lng_sendout_invent(date_process):
                logger.debug(f'tso response: {data}')
                if data['tag'] in ['lmpt1_sendout', 'lmpt1_sendout']:
                    await crud.upsert_eod_value(
                        db=db,
                        item=schemas.EodValueCreate(
                            tag=data['tag'],
                            date=date_process.date(),
                            value=data['value'],
                            update_timestamp=datetime.now()))
                elif data['tag'] == 'lmpt1_invent':
                    lmpt1_invent += data['value']
            await crud.upsert_eod_value(db=db,
                                        item=schemas.EodValueCreate(
                                            tag='lmpt1_invent',
                                            date=date_process.date(),
                                            value=lmpt1_invent,
                                            update_timestamp=datetime.now()))

            data = await pttlng_api.get_eod_lmpt2_invent(
                db, date_process.date())
            logger.debug(f'pttlng response: {data}')
            await crud.upsert_eod_value(db=db,
                                        item=schemas.EodValueCreate(
                                            tag='lmpt2_invent',
                                            date=date_process.date(),
                                            value=data['value'],
                                            update_timestamp=datetime.now()))

            count -= 1
            date_process -= timedelta(days=1)
        except Exception as e:
            logger.error(e)
            count = 0

    logger.info(f'runtime: {runtime() - start_time:3f} s')
    return msg


@router.post("/submit/lng/tank-table")
async def submit_lng_tank_table(file: UploadFile = File(...),
                                db: AsyncSession = Depends(get_db)):
    start_time = runtime()

    # *file validation
    try:
        content = await file.read()
        decoded = content.decode("utf-8-sig")
        csv_reader = csv.DictReader(StringIO(decoded))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'invalidate file')

    REQUIRED_COLUMNS = {'level_cm', 'lmpt2_tank1_m3', 'lmpt2_tank2_m3'}
    actual_columns = set(csv_reader.fieldnames or [])
    logger.debug(actual_columns)
    missing = REQUIRED_COLUMNS - actual_columns
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {', '.join(missing)}")
    # Data Process rows (example: collect to return)
    error_logs = []
    processed_count = 0
    csv_reader = list(csv.DictReader(StringIO(decoded)))
    total_count = len(csv_reader)

    for row in csv_reader:
        try:
            try:
                tank1 = float(row['lmpt2_tank1_m3'])
            except:
                tank1 = None
            try:
                tank2 = float(row['lmpt2_tank2_m3'])
            except:
                tank2 = None
            data = schemas.TankTableCreate(level_cm=int(row['level_cm']),
                                           lmpt2_tank1_m3=tank1,
                                           lmpt2_tank2_m3=tank2)
            await crud.upsert_tank_table(db, data)
        except Exception as e:
            msg = f'{e}, data:{row}'
            logger.error(msg)
            error_logs.append(msg)
        processed_count += 1
        logger.info(f"Processed {processed_count} / {total_count} records")

    logger.info(f'runtime: {runtime() - start_time:3f} s')

    if len(error_logs) == 0:
        return Msg(status='ok', message='insert without error')
    else:
        return Msg(status='error', message=f'errors: {"\n".join(error_logs) }')


@router.post("/submit/eod/value")
async def submit_eod_value(file: UploadFile = File(...),
                           db: AsyncSession = Depends(get_db)):
    start_time = runtime()
    dt = datetime.now()

    # *file validation
    try:
        content = await file.read()
        decoded = content.decode("utf-8-sig")
        csv_reader = csv.DictReader(StringIO(decoded))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'invalidate file')

    REQUIRED_COLUMNS = {'tag', 'date', 'value'}
    actual_columns = set(csv_reader.fieldnames or [])
    logger.debug(actual_columns)
    missing = REQUIRED_COLUMNS - actual_columns
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {', '.join(missing)}")
    # Data Process rows (example: collect to return)
    error_logs = []
    processed_count = 0
    csv_reader = list(csv.DictReader(StringIO(decoded)))
    total_count = len(csv_reader)

    for row in csv_reader:
        try:
            data = schemas.EodValueCreate(tag=row['tag'],
                                          date=datetime.strptime(
                                              row['date'], "%d-%m-%Y").date(),
                                          value=float(row['value']),
                                          update_timestamp=dt)
            await crud.upsert_eod_value(db, data)
        except Exception as e:
            msg = f'{e}, data:{row}'
            logger.error(msg)
            error_logs.append(msg)
        processed_count += 1
        logger.info(f"Processed {processed_count} / {total_count} records")

    logger.info(f'runtime: {runtime() - start_time:3f} s')

    if len(error_logs) == 0:
        return Msg(status='ok', message='insert without error')
    else:
        return Msg(status='error', message=f'errors: {"\n".join(error_logs) }')
