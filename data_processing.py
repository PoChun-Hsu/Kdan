def convert_to_int(value):
    """
    將字串轉換為整數。如果字串包含逗號，會先去除逗號。
    """
    if isinstance(value, int):
        return value
    try:
        return int(value.replace(',', ''))
    except (ValueError, AttributeError):
        return None

def convert_to_float(value):
    """
    將字串轉換為浮點數。如果字串包含逗號，會先去除逗號。
    """
    if isinstance(value, float):
        return value
    try:
        return float(value.replace(',', ''))
    except (ValueError, AttributeError):
        return None

def convert_minguo_to_ad(date_str):
    """
    將民國年日期轉換為西元年日期。日期格式為 'YYY/MM/DD'。
    """
    year, month, day = map(int, date_str.split('/'))
    year += 1911  # 轉換為西元年
    return f'{year}-{month:02d}-{day:02d}'

def date_diff_from_today(date_str):
    """
    計算給定日期與今天日期的天數差異。
    """
    given_date = datetime.strptime(date_str, "%Y-%m-%d")
    current_date = datetime.now()
    return (current_date - given_date).days
