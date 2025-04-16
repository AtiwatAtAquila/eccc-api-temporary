import requests
import logging
import xml.etree.ElementTree as ET
import asyncio
from datetime import datetime, date, time
from fastapi import HTTPException
from app.crud import natural_gas
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings

logger = logging.getLogger(__name__)


def get_current_lmpt1_invent():
    url = 'https://www.pttlngphc.com/api_sendout.php?key=888999'
    response = requests.get(url)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        daily_no = root.find("daily_no")
        invent = 0
        if daily_no is not None:
            invent += float(daily_no.findtext("volume_tank1"))
            invent += float(daily_no.findtext("volume_tank2"))
            invent += float(daily_no.findtext("volume_tank3"))
            invent += float(daily_no.findtext("volume_tank4"))
            timestamp = float(daily_no.findtext("date"))
            dt = datetime.fromtimestamp(timestamp)
            return {'timestamp': dt, 'value': invent}
        else:
            logger.error(f'incorrected structure')

    else:
        logger.error(f'{response}: {response.json()}')
        raise HTTPException(
            status_code=500,
            detail=f'({response.status_code}): {response.text}')


async def get_current_lmpt2_invent(db: AsyncSession):
    params = {
        'keyid': settings.LMPT2_API_KEY,
        'gasday': date.today().strftime('%d/%m/%Y')
    }
    headers = {'Content-Type': 'application/json'}
    url = 'https://tpasystem.pttlng.com/LNGTPA-API/'
    response = requests.post(url, json=params, headers=headers)
    timestamp = datetime.now().replace(minute=0, second=0, microsecond=0)
    level_tank1 = 0
    level_tank2 = 0

    if response.status_code == 200:
        # *get latest value
        for record in response.json():
            if record['DESCRIPTION'] == 'Level-Tank 1-mm.':
                dt = datetime.strptime(record['DATE'], "%Y-%m-%d %H:%M:%S.%f")
                if dt >= timestamp:
                    timestamp = dt
                    level_tank1 = float(record['VALUE'])

            if record['DESCRIPTION'] == 'Level-Tank 2-mm.':
                dt = datetime.strptime(record['DATE'], "%Y-%m-%d %H:%M:%S.%f")
                if dt >= timestamp:
                    timestamp = dt
                    level_tank2 = float(record['VALUE'])

        # *calculate inventory (await async functions)
        logger.debug(f'tank1 level: {level_tank1}, tank2 level: {level_tank2}')
        invent1 = await natural_gas.cal_inventory(db=db,
                                                  level=level_tank1,
                                                  tank=1)
        invent2 = await natural_gas.cal_inventory(db=db,
                                                  level=level_tank2,
                                                  tank=2)
        invent = invent1 + invent2
        logger.info(invent)

        return {'timestamp': timestamp, 'value': invent}

    else:
        logger.error(f'{response}: {response.json()}')
        raise HTTPException(
            status_code=500,
            detail=f'({response.status_code}): {response.text}')


async def get_eod_lmpt2_invent(db: AsyncSession, request_date: date):
    params = {
        'keyid': settings.LMPT2_API_KEY,
        'gasday': request_date.strftime('%d/%m/%Y')
    }
    headers = {'Content-Type': 'application/json'}
    url = 'https://tpasystem.pttlng.com/LNGTPA-API/'

    response = requests.post(url, json=params, headers=headers)

    if response.status_code != 200:
        logger.error(f"API error: {response.status_code} - {response.text}")
        raise HTTPException(status_code=500, detail="LMPT2 API failed")

    records = response.json()
    latest_time = datetime.combine(request_date, time(0, 0))

    # Find latest levels for each tank
    async def find_latest_level(records, desc: str):
        return max(((datetime.strptime(
            rec['DATE'], "%Y-%m-%d %H:%M:%S.%f"), float(rec['VALUE']))
                    for rec in records if rec['DESCRIPTION'] == desc),
                   default=(latest_time, 0),
                   key=lambda x: x[0])

    t1_time, level_tank1 = await find_latest_level(records, 'Level-Tank 1-mm.')
    t2_time, level_tank2 = await find_latest_level(records, 'Level-Tank 2-mm.')
    timestamp = max(t1_time, t2_time)

    logger.debug(f"Latest timestamp: {timestamp}")
    logger.debug(
        f"Tank1 Level: {level_tank1} mm, Tank2 Level: {level_tank2} mm")

    # Await inventory calculations
    invent1, invent2 = await asyncio.gather(
        natural_gas.cal_inventory(db=db, level=level_tank1, tank=1),
        natural_gas.cal_inventory(db=db, level=level_tank2, tank=2))
    total_inventory = invent1 + invent2

    logger.info(f"Calculated Inventory: {total_inventory}")
    return {'timestamp': timestamp, 'value': total_inventory}
