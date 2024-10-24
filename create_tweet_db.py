import sqlite3

DB_PATH = '/home/lighthouse/tweets.db'

def connect_to_db():
    return sqlite3.connect(DB_PATH)

def create_tweets_table(cursor):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS tweets (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Title TEXT,
        Author TEXT,
        CreateTime TEXT,
        UserName TEXT,
        TweetId TEXT UNIQUE,
        Score INTEGER,
        TweetType TEXT
    );
    '''
    cursor.execute(create_table_query)

def create_tweets_v2_table(cursor):
    create_table_v2_query = '''
    CREATE TABLE IF NOT EXISTS tweets_v2 (
        tweetID TEXT PRIMARY KEY,
        Content TEXT,
        CreatedAt TEXT
    );
    '''
    cursor.execute(create_table_v2_query)

def clear_tweets_table(cursor):
    clear_table_query = 'DELETE FROM tweets;'
    cursor.execute(clear_table_query)

def fetch_tweets_v2_data(cursor):
    select_query = 'SELECT * FROM tweets_v2;'
    cursor.execute(select_query)
    return cursor.fetchall()

def print_tweets_v2_data(results):
    print("tweets_v2 表中的所有数据：")
    for row in results:
        print(f"Tweet ID: {row[0]}")
        print(f"Content: {row[1]}")
        print(f"Created At: {row[2]}")
        print("-" * 50)

def recreate_tweets_table(cursor):
    # Drop the existing tweets table
    drop_table_query = 'DROP TABLE IF EXISTS tweets;'
    cursor.execute(drop_table_query)
    
    # Create the new tweets table
    create_table_query = '''
    CREATE TABLE tweets (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Title TEXT,
        Author TEXT,
        CreateTime TEXT,
        UserName TEXT,
        TweetId TEXT UNIQUE,
        Score INTEGER,
        TweetType TEXT
    );
    '''
    cursor.execute(create_table_query)
    print("tweets表已重新创建。")

def get_total_tweets_count(cursor):
    count_query = 'SELECT COUNT(*) FROM tweets;'
    cursor.execute(count_query)
    return cursor.fetchone()[0]

def main():
    conn = connect_to_db()
    cursor = conn.cursor()

    #recreate_tweets_table(cursor)
    
    # Get and print the total number of tweets
    total_tweets = get_total_tweets_count(cursor)
    print(f"tweets表中的总数据数量：{total_tweets}")
    
    conn.commit()

    conn.close()
    print("数据库操作完成。")

if __name__ == "__main__":
    main()
