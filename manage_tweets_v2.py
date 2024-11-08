import sqlite3
import json
from datetime import datetime

DB_PATH = '/home/lighthouse/tweets.db'

def connect_to_db():
    return sqlite3.connect(DB_PATH)

def clear_tweets_v2_table(cursor):
    """清空tweets_v2表"""
    try:
        cursor.execute('DELETE FROM tweets_v2;')
        print("成功清空tweets_v2表")
        return True
    except sqlite3.Error as e:
        print(f"清空表时发生错误: {e}")
        return False

def get_all_tweets(cursor, limit=None):
    """获取所有推文数据"""
    try:
        if limit:
            cursor.execute('SELECT * FROM tweets_v2 ORDER BY CreatedAt DESC LIMIT ?', (limit,))
        else:
            cursor.execute('SELECT * FROM tweets_v2 ORDER BY CreatedAt DESC')
        
        columns = [description[0] for description in cursor.description]
        tweets = []
        
        for row in cursor.fetchall():
            tweet_dict = dict(zip(columns, row))
            # 解析Content字段的JSON
            if tweet_dict['Content']:
                tweet_dict['Content'] = json.loads(tweet_dict['Content'])
            tweets.append(tweet_dict)
            
        return tweets
    except sqlite3.Error as e:
        print(f"获取推文数据时发生错误: {e}")
        return None

def get_tweets_by_date_range(cursor, start_date, end_date):
    """获取指定日期范围内的推文"""
    try:
        query = '''
        SELECT * FROM tweets_v2 
        WHERE CreatedAt BETWEEN ? AND ?
        ORDER BY CreatedAt DESC
        '''
        cursor.execute(query, (start_date, end_date))
        
        columns = [description[0] for description in cursor.description]
        tweets = []
        
        for row in cursor.fetchall():
            tweet_dict = dict(zip(columns, row))
            if tweet_dict['Content']:
                tweet_dict['Content'] = json.loads(tweet_dict['Content'])
            tweets.append(tweet_dict)
            
        return tweets
    except sqlite3.Error as e:
        print(f"获取日期范围内的推文时发生错误: {e}")
        return None

def get_tweets_by_userid(cursor, userid):
    """获取指定用户ID的推文"""
    try:
        cursor.execute('SELECT * FROM tweets_v2 WHERE userid = ? ORDER BY CreatedAt DESC', (userid,))
        
        columns = [description[0] for description in cursor.description]
        tweets = []
        
        for row in cursor.fetchall():
            tweet_dict = dict(zip(columns, row))
            if tweet_dict['Content']:
                tweet_dict['Content'] = json.loads(tweet_dict['Content'])
            tweets.append(tweet_dict)
            
        return tweets
    except sqlite3.Error as e:
        print(f"获取用户推文时发生错误: {e}")
        return None

def get_tweet_by_id(cursor, tweet_id):
    """获取指定ID的推文"""
    try:
        cursor.execute('SELECT * FROM tweets_v2 WHERE tweetID = ?', (tweet_id,))
        
        columns = [description[0] for description in cursor.description]
        row = cursor.fetchone()
        
        if row:
            tweet_dict = dict(zip(columns, row))
            if tweet_dict['Content']:
                tweet_dict['Content'] = json.loads(tweet_dict['Content'])
            return tweet_dict
        return None
    except sqlite3.Error as e:
        print(f"获取推文时发生错误: {e}")
        return None

def print_table_info(cursor):
    """打印表的基本信息"""
    try:
        # 获取表结构
        cursor.execute("PRAGMA table_info(tweets_v2);")
        columns = cursor.fetchall()
        print("\n表结构:")
        for col in columns:
            print(f"列名: {col[1]}, 类型: {col[2]}")
        
        # 获取记录数
        cursor.execute("SELECT COUNT(*) FROM tweets_v2;")
        count = cursor.fetchone()[0]
        print(f"\n总记录数: {count}")
        
        # 获取最早和最新的记录时间
        cursor.execute("SELECT MIN(CreatedAt), MAX(CreatedAt) FROM tweets_v2;")
        min_date, max_date = cursor.fetchone()
        print(f"最早记录时间: {min_date}")
        print(f"最新记录时间: {max_date}")
        
        # 获取不同用户数量
        cursor.execute("SELECT COUNT(DISTINCT userid) FROM tweets_v2 WHERE userid IS NOT NULL AND userid != '';")
        user_count = cursor.fetchone()[0]
        print(f"不同用户数量: {user_count}")
        
    except sqlite3.Error as e:
        print(f"获取表信息时发生错误: {e}")

def main():
    conn = connect_to_db()
    cursor = conn.cursor()
    
    print("=== 数据库管理工具 ===")
    while True:
        print("\n请选择操作:")
        print("1. 清空tweets_v2表")
        print("2. 显示表信息")
        print("3. 获取最新10条推文")
        print("4. 按日期范围查询推文")
        print("5. 按用户ID查询推文")
        print("6. 按推文ID查询推文")
        print("7. 退出")
        
        choice = input("\n请输入选项(1-7): ")
        
        if choice == '1':
            if clear_tweets_v2_table(cursor):
                conn.commit()
                print("表已清空")
        
        elif choice == '2':
            print_table_info(cursor)
        
        elif choice == '3':
            tweets = get_all_tweets(cursor, limit=10)
            if tweets:
                print(f"\n最新{len(tweets)}条推文:")
                for tweet in tweets:
                    print(f"\nTweet ID: {tweet['tweetID']}")
                    print(f"Created At: {tweet['CreatedAt']}")
                    print(f"User ID: {tweet['userid']}")
                    print("-" * 50)
        
        elif choice == '4':
            start_date = input("请输入开始日期 (格式: YYYY-MM-DD): ")
            end_date = input("请输入结束日期 (格式: YYYY-MM-DD): ")
            tweets = get_tweets_by_date_range(cursor, start_date, end_date)
            if tweets:
                print(f"\n找到{len(tweets)}条推文")
                for tweet in tweets:
                    print(f"\nTweet ID: {tweet['tweetID']}")
                    print(f"Created At: {tweet['CreatedAt']}")
                    print(f"User ID: {tweet['userid']}")
                    print("-" * 50)
        
        elif choice == '5':
            userid = input("请输入用户ID: ")
            tweets = get_tweets_by_userid(cursor, userid)
            if tweets:
                print(f"\n找到{len(tweets)}条推文")
                for tweet in tweets:
                    print(f"\nTweet ID: {tweet['tweetID']}")
                    print(f"Created At: {tweet['CreatedAt']}")
                    print("-" * 50)

        elif choice == '6':
            tweet_id = input("请输入推文ID: ")
            tweet = get_tweet_by_id(cursor, tweet_id)
            if tweet:
                print("\n找到推文:")
                print(f"Tweet ID: {tweet['tweetID']}")
                print(f"Created At: {tweet['CreatedAt']}")
                print(f"User ID: {tweet['userid']}")
                print("\n推文内容:")
                print(json.dumps(tweet['Content'], indent=2, ensure_ascii=False))
            else:
                print(f"\n未找到ID为 {tweet_id} 的推文")
        
        elif choice == '7':
            break
        
        else:
            print("无效的选项，请重试")
    
    conn.commit()
    conn.close()
    print("\n程序已退出")

if __name__ == "__main__":
    main() 