import sqlite3
from datetime import datetime
import json

def connect_db():
    """连接到数据库"""
    return sqlite3.connect('/home/lighthouse/tweets.db')

def create_user_table():
    """创建用户表"""
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        # 创建用户表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users_v2 (
                user_id INTEGER PRIMARY KEY,
                screen_name TEXT NOT NULL,
                name TEXT,
                description TEXT,
                location TEXT,
                followers_count INTEGER,
                friends_count INTEGER,
                listed_count INTEGER,
                favourites_count INTEGER,
                media_count INTEGER,
                created_at TEXT,
                profile_image_url TEXT,
                verified BOOLEAN,
                last_updated TEXT
            )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_screen_name ON users_v2(screen_name)')
        
        conn.commit()
        print("User table created successfully")
        
    except Exception as e:
        print(f"Error creating table: {str(e)}")
    finally:
        conn.close()

def insert_sample_user():
    """插入示例用户数据"""
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        sample_user = {
            "id": 20536157,
            "screen_name": "Google",
            "name": "Google",
            "description": "#HeyGoogle",
            "location": "Mountain View, CA",
            "followers_count": 31932829,
            "friends_count": 281,
            "listed_count": 92278,
            "favourites_count": 3688,
            "media_count": 37269,
            "created_at": "Tue Feb 10 19:14:39 +0000 2009",
            "profile_image_url_https": "https://pbs.twimg.com/profile_images/1754606338460487681/bWupXdxo.jpg",
            "verified": False
        }
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
            INSERT OR REPLACE INTO users_v2 (
                user_id, screen_name, name, description, location,
                followers_count, friends_count, listed_count,
                favourites_count, media_count, created_at,
                profile_image_url, verified, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            sample_user['id'],
            sample_user['screen_name'],
            sample_user['name'],
            sample_user['description'],
            sample_user['location'],
            sample_user['followers_count'],
            sample_user['friends_count'],
            sample_user['listed_count'],
            sample_user['favourites_count'],
            sample_user['media_count'],
            sample_user['created_at'],
            sample_user['profile_image_url_https'],
            1 if sample_user['verified'] else 0,
            current_time
        ))
        
        conn.commit()
        print("Sample user inserted successfully")
        
    except Exception as e:
        print(f"Error inserting sample user: {str(e)}")
    finally:
        conn.close()

def query_users():
    """查询用户数据"""
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        # 获取所有用户数量
        cursor.execute('SELECT COUNT(*) FROM users_v2')
        total_users = cursor.fetchone()[0]
        print(f"\nTotal users in database: {total_users}")
        
        # 获取最新添加的5个用户
        print("\nLatest 5 users:")
        cursor.execute('''
            SELECT user_id, screen_name, followers_count, last_updated 
            FROM users_v2 
            ORDER BY last_updated DESC 
            LIMIT 5
        ''')
        latest_users = cursor.fetchall()
        for user in latest_users:
            print(f"ID: {user[0]}, Screen Name: {user[1]}, Followers: {user[2]}, Updated: {user[3]}")
        
        # 获取粉丝数最多的5个用户
        print("\nTop 5 users by followers:")
        cursor.execute('''
            SELECT user_id, screen_name, followers_count, last_updated 
            FROM users_v2 
            ORDER BY followers_count DESC 
            LIMIT 5
        ''')
        top_users = cursor.fetchall()
        for user in top_users:
            print(f"ID: {user[0]}, Screen Name: {user[1]}, Followers: {user[2]}, Updated: {user[3]}")
        
        # 获取特定用户的详细信息
        print("\nDetailed info for 'Google':")
        cursor.execute('SELECT * FROM users_v2 WHERE screen_name = ?', ('Google',))
        user_detail = cursor.fetchone()
        if user_detail:
            columns = [description[0] for description in cursor.description]
            user_dict = dict(zip(columns, user_detail))
            print(json.dumps(user_dict, indent=2))
        else:
            print("User 'Google' not found")
            
    except Exception as e:
        print(f"Error querying users: {str(e)}")
    finally:
        conn.close()

def main():
    """主函数"""
    print("=== Starting User Table Operations ===")
    
    # 创建表
    create_user_table()
    
    # 插入示例数据
    insert_sample_user()
    
    # 查询数据
    query_users()
    
    print("\n=== Operations Completed ===")

if __name__ == "__main__":
    main() 