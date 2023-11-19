import requests
import time
from datetime import datetime, timedelta

def fetch_stock_data(date, stock_no):
    """
    根據指定日期和股票號碼抓取股價數據。
    """
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_no}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    except requests.HTTPError as http_err:
        if http_err.response.status_code in [429, 504]:
            print(f"遇到錯誤碼 {http_err.response.status_code}，將在 60 秒後重試...")
            time.sleep(60)
            return fetch_stock_data(date, stock_no)
        else:
            print(f"HTTP 錯誤發生：{http_err}")
            return None
    except requests.ConnectionError as conn_err:
        print(f"連接錯誤：{conn_err}")
        return None
    except Exception as err:
        print(f"其他錯誤發生：{err}")
        return None

def fetch_stock_data_since_last_update(start_date, end_date, stock_no):
    """
    從最近更新日期到當前日期抓取股票數據。
    """
    try:
        start_datetime = datetime.strptime(start_date, "%Y%m%d")
        end_datetime = datetime.strptime(end_date, "%Y%m%d")

        combined_data = []
        while start_datetime <= end_datetime:
            date_str = start_datetime.strftime("%Y%m%d")
            content = fetch_stock_data(date_str, stock_no)
            if content and 'data' in content:
                combined_data.extend(content['data'])
            
            start_datetime += timedelta(days=1)

        return combined_data

    except Exception as err:
        print(f"抓取數據過程中發生錯誤：{err}")
        return []
