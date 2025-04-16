import requests
import logging
from datetime import datetime
from app.core.config import settings
from requests.auth import HTTPBasicAuth
from fastapi import HTTPException

logger = logging.getLogger(__name__)

user = settings.TSO_API_USER
pwd = settings.TSO_API_PWD


def get_current_supply_mmscfd():
    tags = [
        'GULF-GAS', 'FD-SPE-LNG', 'FD-SPE-LMPT2', 'FD-SPW-MIX_W', 'ESAN-SUPPLY'
    ]
    params = __gen_params(limt=1)
    url = __gen_url(tags=tags)

    logger.debug(url)
    logger.debug(params)

    response = requests.get(url=url,
                            params=params,
                            auth=HTTPBasicAuth(user, pwd))
    if response.status_code == 200:
        try:
            data = response.json()
            for key in data.keys():
                if key == 'GULF-GAS':
                    tag = 'got'
                elif key == 'FD-SPE-LNG' or key == 'FD-SPE-LMPT2':
                    tag = 'lng'
                elif key == 'FD-SPW-MIX_W':
                    tag = 'myanmar'
                elif key == 'ESAN-SUPPLY':
                    tag = 'onshore'
                else:
                    continue

                timestamp_ms = data[key][0]['timestamp']
                yield {
                    'tag': tag,
                    'timestamp': datetime.fromtimestamp(timestamp_ms / 1000),
                    'value': data[key][0]['value']
                }
        except Exception as e:
            logger.exception(e)
    else:
        logger.error(f'{response}: {response.json()}')
        raise HTTPException(
            status_code=500,
            detail=f'({response.status_code}): {response.text}')


def get_current_demand_mmscfd():
    tags = [
        'TOTAL-DEMAND-EAST-EGAT', 'TOTAL-DEMAND-EAST-IPP',
        'TOTAL-DEMAND-EAST-SPP', 'FD-GSP-UGSPRY_TOTAL',
        'TOTAL-DEMAND-EAST-OTHER-IND', 'TOTAL-DEMAND-EAST-OTHER-NGV',
        'TOTAL-DEMAND-EAST-OTHER-FUEL', 'FD-IPP-MIX_WEST',
        'FD_SPP_ONSW_MIX, FD-EGAT-MIX_WEST', 'TOTAL-DEMAND-WEST-OTHER-IND',
        'TOTAL-DEMAND-WEST-OTHER-NGV', 'TOTAL-DEMAND-WEST-OTHER-FUEL',
        'FD-EGAT-CHN, FD-IPP-KN4', 'FLOW-NGV-CHANA', 'FD-GSP-UGSP4',
        'FD-EGAT-NPO, FLOW-NGV-NPO'
    ]
    params = __gen_params(limt=1)
    url = __gen_url(tags=tags)

    logger.debug(url)
    logger.debug(params)

    response = requests.get(url=url,
                            params=params,
                            auth=HTTPBasicAuth(user, pwd))
    if response.status_code == 200:
        try:
            data = response.json()
            for key in data.keys():
                if 'EGAT' in key:
                    tag = 'egat'
                elif 'IPP' in key:
                    tag = 'ipp'
                elif 'SPP' in key:
                    tag = 'spp'
                elif 'GSP' in key:
                    tag = 'gsp'
                elif 'IND' in key:
                    tag = 'ind'
                elif 'NGV' in key:
                    tag = 'ngv'
                elif 'FUEL' in key:
                    tag = 'fuel'
                else:
                    continue

                timestamp_ms = data[key][0]['timestamp']
                yield {
                    'tag': tag,
                    'timestamp': datetime.fromtimestamp(timestamp_ms / 1000),
                    'value': data[key][0]['value']
                }
        except Exception as e:
            logger.exception(e)
    else:
        logger.error(f'{response}: {response.json()}')
        raise HTTPException(
            status_code=500,
            detail=f'({response.status_code}): {response.text}')


def get_lng_sendout_invent(request_date: datetime):
    tags = [
        'ACCF-SPE-LNG', 'ACCF-SPE-LMPT2', 'INVEN_SPE_LNG_A', 'INVEN_SPE_LNG_B',
        'INVEN_SPE_LNG_C', 'INVEN_SPE_LNG_D'
    ]
    params = __gen_params(limt=1, dt=request_date)
    url = __gen_url(tags=tags)

    logger.debug(url)
    logger.debug(params)

    response = requests.get(url=url,
                            params=params,
                            auth=HTTPBasicAuth(user, pwd))
    if response.status_code == 200:
        try:
            data = response.json()
            for key in data.keys():
                if key == 'ACCF-SPE-LNG':
                    tag = 'lmpt1_sendout'
                elif key == 'ACCF-SPE-LMPT2':
                    tag = 'lmpt2_sendout'
                elif 'INVEN' in key:
                    tag = 'lmpt1_invent'
                timestamp_ms = data[key][0]['timestamp']

                dt = datetime.fromtimestamp(timestamp_ms / 1000)
                yield {
                    'tag': tag,
                    'date': dt.strftime('%Y-%m-%d %H:%M %Z'),
                    'value': data[key][0]['value']
                }
        except Exception as e:
            logger.exception(e)
    else:
        logger.error(f'{response}: {response.json()}')
        raise HTTPException(
            status_code=500,
            detail=f'({response.status_code}): {response.text}')


def __gen_url(tags: list):
    url = 'https://gsmgasmonitor.pttplc.com:443/rest/v2/point-values/multiple-arrays/latest/'
    return f'{url}' + ','.join(tags)


def __gen_params(limt: int, dt: datetime = None):
    params = {'limit': limt}
    if dt:
        params['before'] = dt.strftime("%Y-%m-%dT%H:%M:%S%z") + 'Z'

    return params
