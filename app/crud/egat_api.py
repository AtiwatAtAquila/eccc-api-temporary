import requests
import logging
from datetime import date, datetime, timedelta, time
from app.core.config import settings
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


def get_total_lasted_customerDirect():
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetDirectCustomerVal'
    header['st'] = date.today().strftime("%d-%m-%Y")
    header['en'] = date.today().strftime("%d-%m-%Y")

    try:
        response = requests.get(url=url, headers=header)
        data = response.json()['data']['directcus']

        if not data:
            return {'timestamp': None, 'value': 0}

        # Get latest timestamp once
        latest_timestamp = datetime.fromisoformat(data[-1]['TIMESTAMP'])

        # Use generator expression for efficient sum
        total_value = sum(
            float(row['VALUE']) for row in data
            if datetime.fromisoformat(row['TIMESTAMP']) == latest_timestamp)

        return {'timestamp': latest_timestamp, 'value': total_value}

    except Exception as e:
        logger.exception(e)
        return {'timestamp': None, 'value': 0}


def get_total_customerDirect(start_date: date, end_date: date):
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetDirectCustomerVal'
    header['st'] = start_date.today().strftime("%d-%m-%Y")
    header['en'] = end_date.today().strftime("%d-%m-%Y")

    try:
        response = requests.get(url=url, headers=header)
        data = response.json()['data']['directcus']

        result = {}

        for row in data:
            timestamp_str = row['TIMESTAMP']
            timestamp = datetime.fromisoformat(row['TIMESTAMP'])

            if timestamp_str not in result:
                result[timestamp_str] = {
                    'timestamp': timestamp,
                    'value': float(row['VALUE'])
                }
            else:
                result[timestamp_str]['value'] += float(row['VALUE'])
        for index in result.keys():
            data = result[index]
            yield data

    except Exception as e:
        logger.exception(e)


def get_reqMw(index: int, target_date: date):
    # *Get index params
    """
    index = 0 : system gen ค่าประมาณ (20000 - 25000)
    index = 1 REQRCC1: กลาง
    index = 2 REQRCC2: อีสาน
    index = 3 REQRCC3: ใต้
    index = 4 REQRCC4: เหนือ
    index = 5 REQMCC: นครหลวง
    index = 6 EDL_EXP: Export
    index = 7 TNB_EXP: Export
    index = 8 VSPP_PROFILE: vspp ? ค่าประมาณ (300 - 1000)
    index = 9 VSPP_RTU: vspp ? ค่าค้าง 54.4
    index = 10 VSPP_SYSTEM: vspp ? ค่าประมาณ (350 - 1000)
    index = 11 3E_SYSTEM_GEN: system gen ? ค่าประมาณ (มากกว่า index 0)
    index = 12 VSPP_MCC: vspp นครหลวง
    index = 13 VSPP_RCC1: vspp กลาง
    index = 14 VSPP_RCC2: vspp อีสาน
    index = 15 VSPP_RCC3: vspp ใต้
    index = 16 VSPP_RCC4: vspp เหนือ
    """
    params = {'index': index, 'day': target_date.strftime("%d-%m-%Y")}
    url = "https://www.sothailand.com/genws/ws/sysgen/actual"
    response = requests.get(url=url, params=params)

    try:
        return response.json()
    except Exception as e:
        logging.error(f'{response}, error message: {e}')


def get_reqMw_selected_min(index: int, traget_date: date, min: int):
    data = get_reqMw(index, traget_date)
    max_position = len(data['list']) - 1
    if max_position < min:
        position = max_position
    else:
        position = min
    return data['list'][position]


def get_reqMw_profile(index: int, traget_date: date, strat_min, end_min: int,
                      interval_min):
    logger.debug(
        f'start: {strat_min}, end: {end_min}, interval:{interval_min}')
    data = get_reqMw(index, traget_date)
    max_position = len(data['list']) - 1
    if max_position < end_min:
        end_position = max_position
    else:
        end_position = end_min
    position = strat_min
    while end_position < position:
        yield data['list'][position]
        position += interval_min


def get_current_genMw_source3():
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetMwPlantByTime'
    try:
        request_time = datetime.now()
        logger.debug(f'now {request_time}')
        if request_time.minute > 30:
            request_time = request_time.replace(minute=30) - timedelta(
                minutes=30)
        else:
            request_time = request_time.replace(minute=0) - timedelta(
                minutes=30)
        i = 0
        while i < 5:
            header['dt'] = request_time.strftime("%d/%m/%Y %H:%M")
            logger.debug(f'header: {header}')
            response = requests.get(url=url, headers=header)
            data = response.json()
            data = data['data']['MwPlantByTime']
            request_time -= timedelta(minutes=30)
            i += len(data) + 1
            for item in data:
                yield item

    except Exception as e:
        logger.exception(e)


def get_current_genMw_source2():
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetGenMWDataPlantAndTieLine'
    try:
        request_time = datetime.now()
        logger.debug(f'now {request_time}')
        if request_time.minute > 30:
            request_time = request_time.replace(
                minute=30, second=0) - timedelta(minutes=30)
        else:
            request_time = request_time.replace(
                minute=0, second=0) - timedelta(minutes=30)
        i = 0
        if request_time.minute == 0 and request_time.hour == 0:
            process_time = request_time - timedelta(days=1)
        else:
            process_time = request_time
        header['dd'] = process_time.strftime("%d/%m/%Y")
        hour_str = f'F{process_time.hour}'
        if process_time.minute == 30:
            hour_str += 'H'
        logger.debug(f'header: {header}')
        response = requests.get(url=url, headers=header)
        data = response.json()
        for item in data['data']:
            if item['TYPE'] == None:
                continue

            result = {
                'plant_type':
                'imp' if 'IMP' in item['MEANAME'] else item['TYPE'],
                'value': item[hour_str],
                'data_timestamp': request_time
            }
            logger.debug(result)
            yield result

    except Exception as e:
        logger.exception(e)


def get_current_genMw_source1():
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetGenMWData'
    try:
        request_time = datetime.now()
        logger.debug(f'now {request_time}')
        if request_time.minute > 30:
            request_time = request_time.replace(
                minute=30, second=0) - timedelta(minutes=30)
        else:
            request_time = request_time.replace(
                minute=0, second=0) - timedelta(minutes=30)
        i = 0
        if request_time.minute == 0 and request_time.hour == 0:
            process_time = request_time - timedelta(days=1)
        else:
            process_time = request_time
        header['dd'] = process_time.strftime("%d/%m/%Y")
        hour_str = f'F{process_time.hour}'
        if process_time.minute == 30:
            hour_str += 'H'
        logger.debug(f'header: {header}')
        response = requests.get(url=url, headers=header)
        data = response.json()
        for item in data['data']:
            if item['PLANTTYPE'] == None or 'ZZ_SCOD' in item['MEANAME']:
                logger.debug(f'{item['MEANAME']} was skipped')
                continue

            result = {
                'plant_type':
                'imp' if 'IMP' in item['MEANAME'] else item['PLANTTYPE'],
                'value':
                item[hour_str],
                'data_timestamp':
                request_time
            }
            logger.debug(result)
            yield result

    except Exception as e:
        logger.exception(e)


def get_profile_genMw_group_by_type_source2(request_date: date):
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetGenMWDataPlantAndTieLine'
    try:
        dt = datetime.combine(request_date, time(0, 0))
        hour_list = [
            "F0H", "F1", "F1H", "F2", "F2H", "F3", "F3H", "F4", "F4H", "F5",
            "F5H", "F6", "F6H", "F7", "F7H", "F8", "F8H", "F9", "F9H", "F10",
            "F10H", "F11", "F11H", "F12", "F12H", "F13", "F13H", "F14", "F14H",
            "F15", "F15H", "F16", "F16H", "F17", "F17H", "F18", "F18H", "F19",
            "F19H", "F20", "F20H", "F21", "F21H", "F22", "F22H", "F23", "F23H",
            "F0"
        ]
        end_min = 30 * (len(hour_list) + 1)
        dt_list = [dt + timedelta(minutes=i) for i in range(30, end_min, 30)]

        header['dd'] = request_date.strftime("%d/%m/%Y")
        logger.debug(f'header: {header}')
        response = requests.get(url=url, headers=header)
        data = response.json()

        result = {
            'total': {
                'tag': 'total',
                'values': [(dt, 0) for dt in dt_list]
            }
        }

        for item in data['data']:
            plant_type = item['TYPE']
            if not plant_type:
                continue

            if 'IMP' in item['MEANAME']:
                plant_type = 'imp'
            elif 'EGAT' in plant_type:
                plant_type = 'egat'
            elif 'IPP' in plant_type:
                plant_type = 'ipp'
            elif 'SPP' in plant_type:
                plant_type = 'spp'
            else:
                continue

            if plant_type not in result.keys():
                result[plant_type] = {'tag': plant_type, 'values': []}
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    dt = dt_list[i]
                    result[plant_type]['values'].append((dt, item[hour]))
                    old_dt, old_value = result['total']['values'][i]
                    result['total']['values'][i] = (old_dt,
                                                    old_value + item[hour])
            else:
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    old_dt, old_value = result[plant_type]['values'][i]
                    result[plant_type]['values'][i] = (old_dt,
                                                       old_value + item[hour])

                    old_dt, old_value = result['total']['values'][i]
                    result['total']['values'][i] = (old_dt,
                                                    old_value + item[hour])

        for r in result.keys():
            yield result[r]

    except Exception as e:
        logger.exception(e)


def get_profile_genMw_group_by_type_source1(request_date: date):
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetGenMWData'
    try:
        dt = datetime.combine(request_date, time(0, 0))
        hour_list = [
            "F0H", "F1", "F1H", "F2", "F2H", "F3", "F3H", "F4", "F4H", "F5",
            "F5H", "F6", "F6H", "F7", "F7H", "F8", "F8H", "F9", "F9H", "F10",
            "F10H", "F11", "F11H", "F12", "F12H", "F13", "F13H", "F14", "F14H",
            "F15", "F15H", "F16", "F16H", "F17", "F17H", "F18", "F18H", "F19",
            "F19H", "F20", "F20H", "F21", "F21H", "F22", "F22H", "F23", "F23H",
            "F0"
        ]
        end_min = 30 * (len(hour_list) + 1)
        dt_list = [dt + timedelta(minutes=i) for i in range(30, end_min, 30)]

        header['dd'] = request_date.strftime("%d/%m/%Y")
        logger.debug(f'header: {header}')
        response = requests.get(url=url, headers=header)
        data = response.json()

        result = {
            'total': {
                'tag': 'total',
                'values': [(dt, 0) for dt in dt_list]
            }
        }

        for item in data['data']:
            plant_type = item['PLANTTYPE']
            if not plant_type:
                logger.info(f'{item['MEANAME']} was skipped')
                continue

            if 'IMP' in item['MEANAME']:
                plant_type = 'imp'
            elif 'ZZ_SCOD' in item['MEANAME']:
                logger.info(f'{item['MEANAME']} was skipped')
                continue
            elif 'EGAT' in plant_type:
                plant_type = 'egat'
            elif 'IPP' in plant_type:
                plant_type = 'ipp'
            elif 'SPP' in plant_type:
                plant_type = 'spp'
            else:
                logger.info(f'{item['MEANAME']} was skipped')
                continue

            if plant_type not in result.keys():
                result[plant_type] = {'tag': plant_type, 'values': []}
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    dt = dt_list[i]
                    result[plant_type]['values'].append((dt, item[hour]))
                    old_dt, old_value = result['total']['values'][i]
                    result['total']['values'][i] = (old_dt,
                                                    old_value + item[hour])
            else:
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    old_dt, old_value = result[plant_type]['values'][i]
                    result[plant_type]['values'][i] = (old_dt,
                                                       old_value + item[hour])

                    old_dt, old_value = result['total']['values'][i]
                    result['total']['values'][i] = (old_dt,
                                                    old_value + item[hour])

        for r in result.keys():
            yield result[r]

    except Exception as e:
        logger.exception(e)


def get_profile_genMw_group_by_fuel_source1(request_date: date):
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetGenMWData'
    try:
        dt = datetime.combine(request_date, time(0, 0))
        hour_list = [
            "F0H", "F1", "F1H", "F2", "F2H", "F3", "F3H", "F4", "F4H", "F5",
            "F5H", "F6", "F6H", "F7", "F7H", "F8", "F8H", "F9", "F9H", "F10",
            "F10H", "F11", "F11H", "F12", "F12H", "F13", "F13H", "F14", "F14H",
            "F15", "F15H", "F16", "F16H", "F17", "F17H", "F18", "F18H", "F19",
            "F19H", "F20", "F20H", "F21", "F21H", "F22", "F22H", "F23", "F23H",
            "F0"
        ]
        end_min = 30 * (len(hour_list) + 1)
        dt_list = [dt + timedelta(minutes=i) for i in range(30, end_min, 30)]

        header['dd'] = request_date.strftime("%d/%m/%Y")
        logger.debug(f'header: {header}')
        response = requests.get(url=url, headers=header)
        data = response.json()

        result = {}

        for item in data['data']:
            fuel = item['FUEL']
            if not fuel:
                continue

            if 'GAS' in fuel:
                fuel = 'ก๊าซธรรมชาติ'
            elif 'RENEWABLE' in fuel:
                fuel = 'พลังงานทดแทน'
            elif 'HYDRO' in fuel:
                fuel = 'พลังงานน้ำ'
            elif 'COAL' in fuel:
                fuel = 'ถ่านหิน'
            elif 'OIL' in fuel:
                fuel = 'น้ำมัน'
            else:
                logger.debug(f'{fuel} was skipped')
                continue

            if fuel not in result.keys():
                result[fuel] = {'tag': fuel, 'values': []}
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    dt = dt_list[i]
                    result[fuel]['values'].append((dt, item[hour]))

            else:
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    old_dt, old_value = result[fuel]['values'][i]
                    result[fuel]['values'][i] = (old_dt,
                                                 old_value + item[hour])

        for r in result.keys():
            yield result[r]

    except Exception as e:
        logger.exception(e)


def get_profile_genMw_group_by_fuel_source2(request_date: date):
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetGenMWDataPlantAndTieLine'
    try:
        dt = datetime.combine(request_date, time(0, 0))
        hour_list = [
            "F0H", "F1", "F1H", "F2", "F2H", "F3", "F3H", "F4", "F4H", "F5",
            "F5H", "F6", "F6H", "F7", "F7H", "F8", "F8H", "F9", "F9H", "F10",
            "F10H", "F11", "F11H", "F12", "F12H", "F13", "F13H", "F14", "F14H",
            "F15", "F15H", "F16", "F16H", "F17", "F17H", "F18", "F18H", "F19",
            "F19H", "F20", "F20H", "F21", "F21H", "F22", "F22H", "F23", "F23H",
            "F0"
        ]
        end_min = 30 * (len(hour_list) + 1)
        dt_list = [dt + timedelta(minutes=i) for i in range(30, end_min, 30)]

        header['dd'] = request_date.strftime("%d/%m/%Y")
        logger.debug(f'header: {header}')
        response = requests.get(url=url, headers=header)
        data = response.json()

        result = {}

        for item in data['data']:
            fuel = item['FUEL']
            if not fuel:
                continue

            if 'GAS' in fuel:
                fuel = 'ก๊าซธรรมชาติ'
            elif 'RENEWABLE' in fuel:
                fuel = 'พลังงานทดแทน'
            elif 'HYDRO' in fuel:
                fuel = 'พลังงานน้ำ'
            elif 'COAL' in fuel:
                fuel = 'ถ่านหิน'
            elif 'OIL' in fuel:
                fuel = 'น้ำมัน'
            else:
                logger.debug(f'{fuel} was skipped')
                continue

            if fuel not in result.keys():
                result[fuel] = {'tag': fuel, 'values': []}
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    dt = dt_list[i]
                    result[fuel]['values'].append((dt, item[hour]))

            else:
                for i in range(len(hour_list)):
                    hour = hour_list[i]
                    old_dt, old_value = result[fuel]['values'][i]
                    result[fuel]['values'][i] = (old_dt,
                                                 old_value + item[hour])

        for r in result.keys():
            yield result[r]

    except Exception as e:
        logger.exception(e)


def get_gen_mw_by_time(datetime: datetime):
    header = __gen_header()
    url = 'https://www.sothailand.com/PSCODWebAPI/api/StatDataApi/GetDirectCustomerVal'
    header['dt'] = datetime.strftime("%d-%m-%Y %H:%M")
    try:
        response = requests.get(url=url, headers=header)
        data = response.json()['data']['MwPlantByTime']
        return data

    except Exception as e:
        logger.exception(e)


def __gen_header() -> dict:

    url = 'https://www.sothailand.com/PSCODWebAPI/api/LoginApi/GetToken'
    response = requests.get(url=url,
                            auth=HTTPBasicAuth(settings.EGAT_API_USER,
                                               settings.EGAT_API_PWD))
    header = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'token': ''
    }
    response = response.json()
    logger.debug(f'token response {response}')
    try:
        header['token'] = response['access_token']
        return header
    except Exception as e:
        logger.exception(e)
