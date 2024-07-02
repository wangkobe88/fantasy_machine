import sqlite3
from sqlite3 import Error
import csv
import json
import requests

def query_data(db_name, table_name, filter_string):
    """
    Query data from a SQLite database table based on specified criteria.
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # SQL statement for querying
        sql = f'SELECT * FROM {table_name} WHERE {filter_string};'

        # Execute the SQL statement
        cursor.execute(sql)
        result = cursor.fetchall()

        return result

    except Error as e:
        print(f"Error occurred: {e}")
        return None

    finally:
        if conn:
            # Close the database connection
            conn.close()

def count_subrace_occurrences(db_name, table_name, subrace_to_count):
    # 连接到 SQLite 数据库
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 执行统计查询
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE subrace = ?", (subrace_to_count,))
    count = cursor.fetchone()[0]

    # 关闭游标和连接
    cursor.close()
    conn.close()

    return count

def count_rows(db_name, table_name):
    # 连接到 SQLite 数据库
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 执行查询
    query = f"SELECT COUNT(*) FROM {table_name}"
    cursor.execute(query)
    count = cursor.fetchone()[0]

    # 关闭游标和连接
    cursor.close()
    conn.close()

    return count

def count_distinct_rows(db_name, table_name, column):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = f"SELECT COUNT(DISTINCT {column}) FROM {table_name}"
    cursor.execute(query)
    result = cursor.fetchone()[0]

    # 关闭游标和连接
    cursor.close()
    conn.close()
    
    return result

def get_ranked_wallets(db_name, table_name, filter_string):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = f'''
    SELECT * FROM (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank,
            wallet, 
            COUNT(*) as count
        FROM {table_name}
        GROUP BY wallet
    ) 
    '''
    # 如果提供了过滤条件，添加到查询中
    if filter_string:
        query += f" WHERE {filter_string}"

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results

# 增列
def get_json(inscription_id):
    try:    
        base_url = "https://geniidata.com/content/"
        url = base_url + inscription_id
        response = requests.get(url)
        # 检查请求是否成功
        if response.status_code == 200:
            json_content = json.dumps(response.json(), ensure_ascii=False, indent=4)
            data = json.loads(json_content)
            return data
        else:
            return False 
        
    except json.JSONDecodeError:
        return False
