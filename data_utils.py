import pandas as pd
from datetime import datetime

def convert_to_int(value):
    if isinstance(value, int):  # 如果值已經是整數，則直接返回
        return(value,200)
    try:
        return (int(value.replace(',', '')),200)
    except (ValueError, AttributeError):
        return (None,500)
    

def convert_to_float(value):
    if isinstance(value, float):  # 如果值已經是浮點數，則直接返回
        return value
    try:
        return (float(value.replace(',', '')),200)
    except (ValueError, AttributeError):
        return (None,500)


def convert_minguo_to_ad(date_str):
    year, month, day = map(int, date_str.split('/'))
    year += 1911  # 轉換為公元紀年
    return (f'{year}-{month:02d}-{day:02d}',200)
