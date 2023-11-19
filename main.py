import pandas as pd
from datetime import datetime, timedelta
from bigquery_operations import get_latest_date_for_stock, truncate_and_load_table
from data_processing import convert_to_int, convert_to_float, convert_minguo_to_ad
from stock_data_fetcher import fetch_stock_data, fetch_stock_data_since_last_update

def fetch_and_save_stock_data(stock_no):
    try:
        # 獲取最新日期
        table_id = 'starry-center-405211.Kdan.Log_Taiwan_Stock'
        latest_date = get_latest_date_for_stock(client, table_id, stock_no)

        # 如果不存在數據，設置起始日期為 '20231001'
        if latest_date is None:
            start_date = '20231001'
        else:
            start_date = datetime.strftime(latest_date + timedelta(days=1), '%Y%m%d')

        end_date = datetime.now().strftime('%Y%m%d')

        # 抓取數據
        combined_df = fetch_stock_data_since_last_update(start_date, end_date, stock_no)

        # 檢查數據筆數並寫入 BigQuery
        if not combined_df.empty:
            truncate_and_load_table(client, combined_df, table_id, stock_no)

    except json.JSONDecodeError as json_err:
        print(f"JSON 解析錯誤：{json_err}")
    except Exception as err:
        print(f"其他錯誤發生：{err}")

def main(request):
    # 固定參數
    stock_nos = ['2330', '0050']
    for stock_no in stock_nos:
        fetch_and_save_stock_data(stock_no)
    return f'數據處理完成，股票代碼：{stock_nos}'
