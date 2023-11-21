import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import time

from data_utils import convert_to_int, convert_to_float, convert_minguo_to_ad
from bigquery_utils import manage_bigquery_tables, create_temp_table

def fetch_stock_data(date, stock_no):
    try:
        url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_no}'
        response = requests.get(url)
        response.raise_for_status()
        return (response.json(),200)

    except requests.HTTPError as http_err:
        if http_err.response.status_code in [429, 504]:
            print(f"遇到錯誤碼 {http_err.response.status_code}，將在 60 秒後重試...")
            time.sleep(60)
            return (fetch_stock_data(date, stock_no),200)
        else:
            print(f"HTTP 錯誤發生：{http_err}")
            return (None, 500)
    except requests.ConnectionError as conn_err:
        print(f"連接錯誤：{conn_err}")
        return (None, 500)
    except Exception as err:
        print(f"其他錯誤發生：{err}")
        return (None, 500)

def fetch_and_save_stock_data(client, start_date, end_date, stock_no, temp_table_id,):
    try:
        start_datetime = datetime.strptime(start_date, "%Y%m%d")
        end_datetime = datetime.strptime(end_date, "%Y%m%d")
        current_datetime = datetime.now()

        # if start_datetime > end_datetime:
        #     print("起始日期應小於結束日期。")
        #     return
        # if start_datetime > current_datetime:
        #     print("起始日期應小於當前日期。")
        #     return

        # if end_datetime > current_datetime:
        #     end_datetime = current_datetime
        #     print(f"結束日期超出範圍，已調整為最新日期：{current_datetime.strftime('%Y%m%d')}")

        combined_df = pd.DataFrame()
        update_timestamp = datetime.now()

        while start_datetime <= end_datetime:
            date_str = start_datetime.strftime("%Y%m01")
            content = fetch_stock_data(date_str, stock_no)
            if content is None:
                continue

            stock_data = content['data']
            col_name = content['fields']

            # 插入 Stock_Code 和 Update_Timestamp
            for row in stock_data:
                row.insert(1, stock_no)  # 插入 Stock_Code
                row.append(update_timestamp)  # 添加 Update_Timestamp

            # 轉換欄位名稱
            col_name.insert(1, 'Stock_Code')
            col_name.append('Update_Timestamp')

            col_name_mapping = {
                "日期": "Date",
                "成交股數": "Number_of_Shares_Traded",
                "成交金額": "Total_Trade_Value",
                "開盤價": "Opening_Price",
                "最高價": "Highest_Price",
                "最低價": "Lowest_Price",
                "收盤價": "Closing_Price",
                "漲跌價差": "Price_Difference",
                "成交筆數": "Number_of_Transactions"
            }

            df = pd.DataFrame(stock_data, columns=col_name)
            df.rename(columns=col_name_mapping, inplace=True)

            # 數據類型轉換
            # 將 'Date' 欄位從民國年轉換為公元年
            df['Date'] = df['Date'].apply(convert_minguo_to_ad)

            for col in ['Number_of_Shares_Traded', 'Total_Trade_Value', 'Number_of_Transactions']:
                df[col] = df[col].apply(convert_to_int)

            for col in ['Opening_Price', 'Highest_Price', 'Lowest_Price', 'Closing_Price', 'Price_Difference']:
                df[col] = df[col].apply(convert_to_float)

            combined_df = pd.concat([combined_df, df])

            start_datetime += timedelta(days=32)
            start_datetime = start_datetime.replace(day=1)

        # 將數據寫入 BigQuery
        # 將數據寫入臨時表格
        job = client.load_table_from_dataframe(combined_df, temp_table_id)
        job.result()

        #print(f"數據已儲存至 BigQuery 表格：{table_id}")
        return (None, 200)
    except json.JSONDecodeError as json_err:
        print(f"JSON 解析錯誤：{json_err}")
        return (None, 500)
    except Exception as err:
        print(f"其他錯誤發生：{err}")
        return (None, 500)


def fetch_and_save_multiple_stocks(client, start_date, end_date, stock_nos, temp_table_id, source_table_id, backup_table_id):
    create_temp_table(client, backup_table_id, temp_table_id)
    
    for stock_no in stock_nos:
        fetch_and_save_stock_data(client, start_date, end_date, stock_no, temp_table_id)

    # 完成臨時表格的數據寫入後，調用 manage_bigquery_tables 函數
    manage_bigquery_tables(client, source_table_id, backup_table_id)

    # 將臨時表格的數據轉移到最終表格
    client.copy_table(temp_table_id, source_table_id)
    print(f"表格 {temp_table_id} 已重命名為 {source_table_id}")
    client.delete_table(temp_table_id, not_found_ok=True)

    return (None, 200)
