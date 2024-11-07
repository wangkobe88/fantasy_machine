import sqlite3

DB_PATH = '/home/lighthouse/tweets.db'

def connect_to_db():
    return sqlite3.connect(DB_PATH)

def alter_tweets_v2_table(cursor):
    try:
        # 1. 创建临时表
        create_temp_table_query = '''
        CREATE TABLE tweets_v2_temp (
            tweetID TEXT PRIMARY KEY,
            Content TEXT,
            CreatedAt TEXT,
            userid TEXT
        );
        '''
        cursor.execute(create_temp_table_query)

        # 2. 复制原有数据到临时表
        copy_data_query = '''
        INSERT INTO tweets_v2_temp (tweetID, Content, CreatedAt)
        SELECT tweetID, Content, CreatedAt FROM tweets_v2;
        '''
        cursor.execute(copy_data_query)

        # 3. 删除原表
        drop_original_table_query = 'DROP TABLE tweets_v2;'
        cursor.execute(drop_original_table_query)

        # 4. 将临时表重命名为原表名
        rename_table_query = 'ALTER TABLE tweets_v2_temp RENAME TO tweets_v2;'
        cursor.execute(rename_table_query)

        print("成功添加userid字段到tweets_v2表")
        return True

    except sqlite3.Error as e:
        print(f"发生错误: {e}")
        return False

def verify_table_structure(cursor):
    # 验证表结构
    cursor.execute("PRAGMA table_info(tweets_v2);")
    columns = cursor.fetchall()
    print("\n当前tweets_v2表结构:")
    for col in columns:
        print(f"列名: {col[1]}, 类型: {col[2]}")

def count_records(cursor):
    # 统计记录数
    cursor.execute("SELECT COUNT(*) FROM tweets_v2;")
    count = cursor.fetchone()[0]
    print(f"\n当前tweets_v2表中的记录数: {count}")

def main():
    conn = connect_to_db()
    cursor = conn.cursor()

    print("开始修改tweets_v2表...")
    
    # 在修改之前统计记录数
    print("\n修改前:")
    verify_table_structure(cursor)
    count_records(cursor)

    # 执行表修改
    if alter_tweets_v2_table(cursor):
        # 在修改之后验证
        print("\n修改后:")
        verify_table_structure(cursor)
        count_records(cursor)
    
    conn.commit()
    conn.close()
    print("\n数据库操作完成。")

if __name__ == "__main__":
    main() 