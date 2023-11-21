from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from stock_data import fetch_and_save_multiple_stocks

# 使用範例
def main():
    # 固定參數
    start_date = '20231001'
    end_date = datetime.now().strftime('%Y%m%d')
    stock_nos = ['2330', '0050']

    temp_table_id = 'starry-center-405211.Kdan.Log_Taiwan_Stock_Temp'
    source_table_id = 'starry-center-405211.Kdan.Log_Taiwan_Stock'
    backup_table_id = 'starry-center-405211.Kdan.Log_Taiwan_Stock_Backup'

    # 創建 BigQuery 客戶端
    project_id = 'starry-center-405211'  # 將 '您的專案 ID' 替換為您的 Google Cloud 專案 ID
    client = bigquery.Client(project=project_id)
    
    try:
        fetch_and_save_multiple_stocks(client, start_date, end_date, stock_nos, temp_table_id, source_table_id, backup_table_id)
        return ("數據處理完成，範圍：{} 至 {}，股票代碼：{}".format(start_date, end_date, stock_nos), 200)
    except Exception as e:
        return ("處理過程中出現錯誤: {}".format(str(e)), 400)
