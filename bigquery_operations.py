from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# 創建 BigQuery 客戶端
project_id = 'starry-center-405211'  # 將 '您的專案 ID' 替換為您的 Google Cloud 專案 ID
client = bigquery.Client(project=project_id)

def get_latest_date_for_stock(client, table_id, stock_no):
    """
    獲取特定股票代碼的最新日期。
    """
    query = f"""
        SELECT MAX(Date) as latest_date
        FROM `{table_id}`
        WHERE Stock_Code = '{stock_no}'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row.latest_date


def truncate_and_load_table(client, df, table_id, stock_no):
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"新數據已寫入表格 {table_id}。")
