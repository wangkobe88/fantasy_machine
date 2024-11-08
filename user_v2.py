from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import traceback
import os

app = Flask(__name__)
app.config['DEBUG'] = True

def connect_db():
    """连接到数据库"""
    return sqlite3.connect('/home/lighthouse/tweets.db')

@app.route('/add_users', methods=['POST'])
def add_users():
    print("\n=== add_users function called ===")
    print(f"Time: {datetime.now(ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    try:
        data = request.json
        print("\n=== Received Full Data ===")
        print(json.dumps(data, indent=2))
        print("=== End of Full Data ===\n")

        # 将请求数据保存到文件
        log_dir = '/home/lighthouse/logs/user_requests'
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f'{log_dir}/user_request_{timestamp}.json'
        
        with open(log_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Request data saved to {log_file}")

        # 验证数据结构
        print("\n=== Validating Data Structure ===")
        if not data or 'output' not in data or not isinstance(data['output'], list):
            print("Error: Invalid JSON data received")
            return jsonify({"error": "Invalid JSON data received"}), 400

        # 提取用户数据
        users = []
        processed_user_ids = set()  # 用于跟踪已处理的用户ID
        print("\n=== Extracting User Data ===")
        
        for i, item in enumerate(data['output']):
            print(f"\nProcessing output item {i+1}/{len(data['output'])}")
            
            if not item.get('data') or not isinstance(item.get('data'), dict):
                print(f"Skipping item {i+1}: Invalid data structure")
                continue
                
            item_users = item['data'].get('users')
            if not item_users:
                print(f"Skipping item {i+1}: No users data")
                continue
                
            for user in item_users:
                user_id = user.get('id')
                if not user_id:
                    print("Skipping user: Missing ID")
                    continue
                    
                if user_id in processed_user_ids:
                    print(f"Skipping duplicate user ID: {user_id}")
                    continue
                    
                users.append(user)
                processed_user_ids.add(user_id)
                print(f"Added user {user_id} to processing queue")

        if not users:
            print("Error: No valid users data found")
            return jsonify({"error": "No valid users data found"}), 400

        print(f"\nTotal unique users extracted: {len(users)}")

        # 数据库操作
        print("\n=== Database Operations ===")
        conn = connect_db()
        cursor = conn.cursor()

        inserted_users = []
        error_users = []

        # 处理每个用户
        print("\n=== Processing Users ===")
        for i, user in enumerate(users):
            try:
                print(f"\nProcessing user {i+1}/{len(users)}")
                user_id = user.get('id')
                
                if not user_id:
                    print("Error: Missing user ID")
                    raise ValueError("Missing user ID")
                
                print(f"User ID: {user_id}")
                print(f"Screen Name: {user.get('screen_name')}")

                current_time = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
                
                user_data = (
                    user_id,
                    user.get('screen_name'),
                    user.get('name'),
                    user.get('description'),
                    user.get('location'),
                    user.get('followers_count'),
                    user.get('friends_count'),
                    user.get('listed_count'),
                    user.get('favourites_count'),
                    user.get('media_count'),
                    user.get('created_at'),
                    user.get('profile_image_url_https'),
                    1 if user.get('verified') else 0,
                    current_time
                )
                
                print("Prepared user data:")
                print(json.dumps({
                    'user_id': user_data[0],
                    'screen_name': user_data[1],
                    'followers_count': user_data[5],
                    'friends_count': user_data[6],
                    'last_updated': user_data[13]
                }, indent=2))

                # 插入新记录
                cursor.execute('''
                    INSERT INTO users_v2 (
                        user_id, screen_name, name, description, location,
                        followers_count, friends_count, listed_count,
                        favourites_count, media_count, created_at,
                        profile_image_url, verified, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', user_data)
                
                inserted_users.append(user_id)
                print(f"Inserted new record for user {user_id}")

            except sqlite3.IntegrityError as e:
                print(f"\nDatabase integrity error for user {user_id}: {str(e)}")
                print(f"Full error traceback: {traceback.format_exc()}")
                error_users.append(str(user_id))
            except Exception as e:
                print(f"\nError processing user: {str(e)}")
                print(f"Full error traceback: {traceback.format_exc()}")
                print(f"User data: {json.dumps(user, indent=2)}")
                error_users.append(str(user_id) if user_id else "Unknown ID")

        # 提交事务
        print("\n=== Finalizing Database Operations ===")
        print("Committing changes...")
        conn.commit()
        print("Changes committed successfully")
        conn.close()
        print("Database connection closed")

        # 返回结果
        print("\n=== Operation Summary ===")
        print(f"Total processed: {len(users)}")
        print(f"Inserted: {len(inserted_users)}")
        print(f"Errors: {len(error_users)}")

        return jsonify({
            "inserted": inserted_users,
            "errors": error_users,
            "total_processed": len(users),
            "total_inserted": len(inserted_users),
            "total_errors": len(error_users)
        }), 200

    except Exception as e:
        print(f"\n=== Unexpected Error ===")
        print(f"Error message: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

@app.route('/get_user_follower_averages', methods=['GET'])
def get_user_follower_averages():
    print("get_user_follower_averages function called")
    try:
        conn = connect_db()
        cursor = conn.cursor()

        now = datetime.now(ZoneInfo("UTC"))
        
        time_ranges = {
            '3d': 3,
            '7d': 7,
            '15d': 15,
            '30d': 30,
            '90d': 90
        }

        user_follower_averages = {}

        for range_key, days in time_ranges.items():
            start_date = (now - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            query = '''
                SELECT user_id, screen_name, AVG(followers_count) as avg_followers
                FROM users_v2
                WHERE last_updated >= ?
                GROUP BY user_id, screen_name
            '''
            cursor.execute(query, (start_date,))
            rows = cursor.fetchall()

            for row in rows:
                user_id, screen_name, avg_followers = row
                if user_id not in user_follower_averages:
                    user_follower_averages[user_id] = {
                        'screen_name': screen_name,
                        'averages': {}
                    }
                user_follower_averages[user_id]['averages'][range_key] = avg_followers

        formatted_averages = []
        for user_id, data in user_follower_averages.items():
            formatted_averages.append({
                'user_id': user_id,
                'screen_name': data['screen_name'],
                'averages': data['averages']
            })

        return jsonify({
            'total_users': len(formatted_averages),
            'updated_at': now.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'averages': formatted_averages
        }), 200

    except Exception as e:
        print(f"Error in get_user_follower_averages: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# API to get user statistics
@app.route('/get_user_stats', methods=['GET'])
def get_user_stats():
    print("get_user_stats function called")
    try:
        conn = connect_db()
        cursor = conn.cursor()

        # 获取当前UTC时间
        now = datetime.now(ZoneInfo("UTC"))
        
        # 定义时间范围
        time_ranges = {
            '3d': 3,
            '7d': 7,
            '15d': 15,
            '30d': 30,
            '90d': 90
        }

        # 获取所有用户的推文
        query = """
        SELECT Content, CreatedAt 
        FROM tweets_v2 
        WHERE CreatedAt >= datetime('now', '-90 days')
        ORDER BY CreatedAt DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # 用户统计数据
        user_stats = {}

        # 处理每条推文
        for row in rows:
            content = json.loads(row[0])
            created_at = datetime.strptime(row[1], "%a %b %d %H:%M:%S %z %Y")
            
            user = content.get('user', {})
            screen_name = user.get('screen_name')

            if not screen_name:
                continue

            if screen_name not in user_stats:
                user_stats[screen_name] = {
                    '3d': 0,
                    '7d': 0,
                    '15d': 0,
                    '30d': 0,
                    '90d': 0
                }

            # 计算天数差
            days_diff = (now - created_at).days

            # 更新各时间范围的统计
            for range_key, days in time_ranges.items():
                if days_diff <= days:
                    user_stats[screen_name][range_key] += 1

        # 格式化结果
        formatted_stats = []
        for screen_name, tweet_counts in user_stats.items():
            user_data = {
                'screen_name': screen_name,
                'tweet_counts': tweet_counts
            }
            formatted_stats.append(user_data)

        # 按90天推文数量排序
        formatted_stats.sort(key=lambda x: x['tweet_counts']['90d'], reverse=True)

        return jsonify({
            'total_users': len(formatted_stats),
            'updated_at': now.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'stats': formatted_stats
        }), 200

    except Exception as e:
        print(f"Error in get_user_stats: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5010, debug=True)
