from google.cloud import bigquery
from google.cloud.bigquery import Table
from google.cloud.exceptions import NotFound

def manage_bigquery_tables(client, source_table_id, backup_table_id):
    try:
        # Step 1: 刪除 backup 表格（如果存在）
        client.delete_table(backup_table_id, not_found_ok=True)

        # Step 2: 重命名原表格為 backup
        client.copy_table(source_table_id, backup_table_id)
        client.delete_table(source_table_id, not_found_ok=True)

        print(f"表格 {source_table_id} 已重命名為 {backup_table_id}")
        return (None, 200)
    except Exception as e:
        print(f"BigQuery 表格操作錯誤: {e}")
        return (None, 500)

def table_exists(client, table_id):
    try:
        client.get_table(table_id)  # 使用 get_table 嘗試獲取表格
        return (True,200)
    except NotFound:
        return (False,500)  # 如果表格不存在，返回 False


def create_temp_table(client, source_table_id, temp_table_id):
    try:
        # 檢查臨時表格是否存在，如果存在則刪除
        if table_exists(client, temp_table_id):
            client.delete_table(temp_table_id)
            print(f"已存在的表格 {temp_table_id} 已刪除。")

        # 獲取原表格的結構
        source_table = client.get_table(source_table_id)

        # 創建一個新的 Table 實例，並將結構設定為源表格的結構
        temp_table = Table(temp_table_id, schema=source_table.schema)
        
        # 設定分簇字段
        temp_table.clustering_fields = ["Date", "Stock_Code"]
        # 創建表格
        client.create_table(temp_table)
        print(f"表格 {temp_table_id} 已創建。")

        return (None, 200)
    except Exception as e:
        print(f"創建表格時出錯: {e}")
        return (None, 500)
