from flask import Flask, request, jsonify, Response
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import traceback
import os

app = Flask(__name__)
app.config['DEBUG'] = True  # Enable debug mode

import re


def extract_username(url):
    # 使用正则表达式来匹配URL中的用户名
    pattern = r"https?://(?:www\.)?twitter\.com/([^/]+)/status/\d+"
    match = re.search(pattern, url)

    if match:
        return match.group(1)
    else:
        return None

# Database connection
def connect_db():
    conn = sqlite3.connect('/home/lighthouse/tweets.db')
    return conn


# API to insert multiple tweets into the database
@app.route('/add_tweets', methods=['POST'])
def add_tweets():
    print("add_tweets function called")
    try:
        data = request.json
        if not data:
            print("No JSON data received")
            return jsonify({"error": "No JSON data received"}), 400

        print(f"Received {len(data)} tweets")
        conn = connect_db()
        cursor = conn.cursor()

        inserted_tweets = []
        skipped_tweets = []

        for index, tweet in enumerate(data):
            try:
                print(f"Processing tweet {index + 1}/{len(data)}")
                tweet_id = tweet['rest_id']
                content = json.dumps(tweet)
                created_at = tweet.get('created_at')
                # 从tweet中提取userid
                userid = str(tweet.get('user', {}).get('id_str', ''))

                print(f"Tweet ID: {tweet_id}, Created At: {created_at}, User ID: {userid}")

                # Check if a tweet with the same tweetID exists
                cursor.execute('SELECT tweetID FROM tweets_v2 WHERE tweetID = ?', (tweet_id,))
                result = cursor.fetchone()

                if result:
                    print(f"Tweet {tweet_id} already exists, skipping")
                    skipped_tweets.append(tweet_id)
                else:
                    cursor.execute('''
                        INSERT INTO tweets_v2 (tweetID, Content, CreatedAt, userid) 
                        VALUES (?, ?, ?, ?)
                    ''', (tweet_id, content, created_at, userid))
                    print(f"Tweet {tweet_id} inserted successfully")
                    inserted_tweets.append(tweet_id)
            except KeyError as ke:
                print(f"KeyError processing tweet {index + 1}: {ke}")
                print(f"Tweet data: {tweet}")
                return jsonify({"error": f"Missing key in tweet data: {ke}"}), 400
            except Exception as e:
                print(f"Error inserting tweet {tweet.get('rest_id', 'Unknown ID')}: {str(e)}")
                print(f"Tweet data: {tweet}")
                conn.rollback()
                return jsonify({"error": f"Error inserting tweet {tweet.get('rest_id', 'Unknown ID')}: {str(e)}"}), 500

        conn.commit()
        conn.close()

        print(f"Operation completed. Inserted: {len(inserted_tweets)}, Skipped: {len(skipped_tweets)}")
        return jsonify({
            "inserted": inserted_tweets, 
            "skipped": skipped_tweets,
            "total_processed": len(data),
            "total_inserted": len(inserted_tweets),
            "total_skipped": len(skipped_tweets)
        }), 200

    except Exception as e:
        print(f"Unexpected error in add_tweets: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

def get_influence_level(influence):
    try:
        influence_value = int(influence)
        if influence_value == 1:
            return "低"
        elif influence_value == 2:
            return "中"
        elif influence_value == 3:
            return "高"
        elif influence_value > 3:
            return "超高"
        else:
            return "未知"
    except ValueError:
        return "未知"

# API to get today's tweets
@app.route('/get_tweets', methods=['GET'])
def get_tweets():
    print("get_tweets function called")
    conn = connect_db()
    cursor = conn.cursor()

    # Use UTC time for consistency
    now = datetime.now(ZoneInfo("UTC"))
    today = now.strftime('%Y-%m-%d')
    tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"Querying for tweets from {today} and {tomorrow}")

    # 读取 meme_kols.csv 文件
    meme_kols = {}
    with open('./data/meme_kols.csv', 'r') as f:
        next(f)  # 跳过标题行
        for line in f:
            username, influence = line.strip().split(',')[:2]
            meme_kols[username.lower()] = influence

    try:
        # Fetch the latest 500 tweets
        query = """
        SELECT Content, CreatedAt FROM tweets_v2 
        ORDER BY CreatedAt DESC
        LIMIT 500
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"Fetched {len(rows)} tweets")

        # Filter tweets for today and tomorrow
        filtered_tweets = []
        for row in rows:
            content = json.loads(row[0])
            created_at = row[1]
            tweet_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y").strftime('%Y-%m-%d')
            if tweet_date in [today, tomorrow]:
                filtered_tweets.append(content)

        print(f"Filtered to {len(filtered_tweets)} tweets for today and tomorrow")

        if not filtered_tweets:
            print("No tweets found for today or tomorrow")
            return jsonify({"error": "No tweets found for today or tomorrow"}), 404

        formatted_tweets = []
        for tweet_data in filtered_tweets:
            full_text = tweet_data.get('full_text', '')
            # 解码 full_text
            full_text = full_text.encode().decode('unicode_escape')
            
            name = tweet_data['user']['name']
            # 解码 name
            name = name.encode().decode('unicode_escape')
            
            screen_name = tweet_data['user']['screen_name']
            created_at = tweet_data['created_at']
            tweet_id = tweet_data['rest_id']
            
            create_time_obj = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            create_time_obj = create_time_obj.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Shanghai"))
            create_time_cn = create_time_obj.strftime("%Y年%m月%d日 %H:%M:%S")
            
            link = f"https://twitter.com/{screen_name}/status/{tweet_id}"
            
            # 获取影响力信息并转换
            influence = meme_kols.get(screen_name.lower(), "未知")
            influence_level = get_influence_level(influence)
            
            formatted_tweets.append({
                "text": full_text,
                "author": {
                    "name": name,
                    "screen_name": screen_name
                },
                "created_at": create_time_cn,
                "id": tweet_id,
                "link": link,
                "influence": influence_level
            })

        return jsonify({
            "tweets": formatted_tweets,
            "total": len(formatted_tweets),
            "updated_at": now.strftime('%Y-%m-%d %H:%M:%S')
        }), 200

    except Exception as e:
        print(f"Error in get_tweets: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# API to get today's tweets formatted for Twitter posting
@app.route('/get_tweets_formated', methods=['GET'])
def get_tweets_formated():
    conn = connect_db()
    cursor = conn.cursor()

    # Use UTC time for consistency
    now = datetime.now(ZoneInfo("UTC"))
    today = now.strftime('%Y-%m-%d')
    tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"Querying for tweets from {today} and {tomorrow}")

    # 读取 meme_kols.csv 文件
    meme_kols = {}
    with open('./data/meme_kols.csv', 'r') as f:
        next(f)  # 跳过标题行
        for line in f:
            username, influence = line.strip().split(',')[:2]
            meme_kols[username.lower()] = influence

    try:
        # Fetch the latest 200 tweets
        query = """
        SELECT Content, CreatedAt FROM tweets_v2 
        ORDER BY CreatedAt DESC
        LIMIT 500
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"Fetched {len(rows)} tweets")

        # Filter tweets for today and tomorrow
        filtered_tweets = []
        for row in rows:
            content = json.loads(row[0])
            created_at = row[1]
            tweet_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y").strftime('%Y-%m-%d')
            if tweet_date in [today, tomorrow]:
                filtered_tweets.append(content)

        print(f"Filtered to {len(filtered_tweets)} tweets for today and tomorrow")

        if not filtered_tweets:
            print("No tweets found for today or tomorrow")
            # Add debug information
            cursor.execute("SELECT COUNT(*) FROM tweets_v2")
            total_tweets = cursor.fetchone()[0]
            print(f"Total tweets in database: {total_tweets}")
            
            cursor.execute("SELECT MIN(CreatedAt), MAX(CreatedAt) FROM tweets_v2")
            min_date, max_date = cursor.fetchone()
            print(f"Date range in database: from {min_date} to {max_date}")

            # Add a query to fetch a few sample tweets
            cursor.execute("SELECT tweetID, CreatedAt, substr(Content, 1, 100) FROM tweets_v2 ORDER BY CreatedAt DESC LIMIT 5")
            sample_tweets = cursor.fetchall()
            print("Sample tweets:")
            for tweet in sample_tweets:
                print(f"ID: {tweet[0]}, Created At: {tweet[1]}, Content preview: {tweet[2]}...")

            return Response("今天或明天没有找到推文。", mimetype='text/html')

        html_content = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>最新热门推文</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 800px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                h1 {{
                    color: #1da1f2;
                    text-align: center;
                }}
                .tweet {{
                    background-color: #f8f9fa;
                    border: 1px solid #e1e8ed;
                    border-radius: 10px;
                    padding: 15px;
                    margin-bottom: 20px;
                }}
                .tweet-text {{
                    font-size: 16px;
                    margin-bottom: 10px;
                }}
                .tweet-info {{
                    font-size: 14px;
                    color: #657786;
                }}
                .tweet-link {{
                    color: #1da1f2;
                    text-decoration: none;
                }}
                .tweet-link:hover {{
                    text-decoration: underline;
                }}
            </style>
        </head>
        <body>
            <h1>🔥 最新热门推文 🔥</h1>
            <p>更新时间: {now.strftime('%Y年%m月%d日 %H:%M:%S')} 北京时间</p>
        """

        for tweet_data in filtered_tweets:
            full_text = tweet_data.get('full_text', '')
            # 解码 full_text
            full_text = full_text.encode().decode('unicode_escape')
            
            name = tweet_data['user']['name']
            # 解码 name
            name = name.encode().decode('unicode_escape')
            
            screen_name = tweet_data['user']['screen_name']
            created_at = tweet_data['created_at']
            tweet_id = tweet_data['rest_id']
            
            create_time_obj = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            create_time_obj = create_time_obj.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Shanghai"))
            create_time_cn = create_time_obj.strftime("%Y年%m月%d日 %H:%M:%S")
            
            link = f"https://twitter.com/{screen_name}/status/{tweet_id}"
            
            # 获取影响力信息并转换
            influence = meme_kols.get(screen_name.lower(), "未知")
            influence_level = get_influence_level(influence)
            
            html_content += f"""
            <div class="tweet">
                <div class="tweet-text">{full_text}</div>
                <div class="tweet-info">
                    <p>👤 作者: {name} @{screen_name}</p>
                    <p>🕒 时间: {create_time_cn}</p>
                    <p>🔗 链接: <a href="{link}" target="_blank" class="tweet-link">{link}</a></p>
                    <p>🌟 影响力: {influence_level}</p>
                </div>
            </div>
            """

        html_content += """
        </body>
        </html>
        """

        return Response(html_content, mimetype='text/html')

    except Exception as e:
        print(f"Error in get_todays_tweets_formated: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return Response(f"<h1>发生错误</h1><p>{str(e)}</p>", mimetype='text/html', status=500)
    finally:
        conn.close()


def save_raw_data(data, prefix="tweets"):
    """保存原始数据到文件"""
    try:
        # 创建 raw_data 目录（如果不存在）
        data_dir = "/home/lighthouse/raw_data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_dir}/{prefix}_{timestamp}.json"
        
        # 保存数据
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"Raw data saved to: {filename}")
        return filename
    except Exception as e:
        print(f"Error saving raw data: {str(e)}")
        return None

# API to add all tweets
@app.route('/add_all_tweets', methods=['POST'])
def add_all_tweets():
    print("add_all_tweets function called")
    try:
        data = request.json
        print("=== Received Full Data ===")
        print(json.dumps(data, indent=2))
        print("=== End of Full Data ===\n")

        # 保存原始数据
        saved_file = save_raw_data(data)
        if saved_file:
            print(f"Raw data saved to: {saved_file}")

        if not data or not isinstance(data, dict) or 'output' not in data:
            print("Invalid JSON data received - expected an object with 'output' field")
            return jsonify({"error": "Invalid JSON data received"}), 400

        output_list = data.get('output', [])
        if not isinstance(output_list, list):
            print("Invalid output data - expected a list")
            return jsonify({"error": "Invalid output data structure"}), 400

        conn = connect_db()
        cursor = conn.cursor()

        inserted_tweets = []
        skipped_tweets = []
        error_tweets = []
        
        # 遍历输出列表中的每个项
        for item_index, item in enumerate(output_list):
            try:
                free_busy = item.get('data', {}).get('freeBusy')
                
                if free_busy is None:
                    print(f"Item {item_index + 1}: freeBusy is None, skipping")
                    continue

                tweets = free_busy.get('post', [])
                if not tweets:
                    print(f"Item {item_index + 1}: No tweets found")
                    continue

                print(f"Found {len(tweets)} tweets in item {item_index + 1}")

                for tweet_index, tweet in enumerate(tweets):
                    try:
                        tweet_id = tweet.get('rest_id')
                        if not tweet_id:
                            print(f"Missing tweet ID for tweet {tweet_index + 1}")
                            raise ValueError("Missing tweet ID")

                        content = json.dumps(tweet)
                        created_at = tweet.get('created_at')
                        userid = str(tweet.get('user', {}).get('rest_id', ''))

                        print(f"Processing tweet {tweet_index + 1}/{len(tweets)}")
                        print(f"Tweet ID: {tweet_id}, Created At: {created_at}, User ID: {userid}")

                        # Check if tweet exists
                        cursor.execute('SELECT tweetID FROM tweets_v2 WHERE tweetID = ?', (tweet_id,))
                        result = cursor.fetchone()

                        if result:
                            print(f"Tweet {tweet_id} already exists, skipping")
                            skipped_tweets.append(tweet_id)
                        else:
                            cursor.execute('''
                                INSERT INTO tweets_v2 (tweetID, Content, CreatedAt, userid) 
                                VALUES (?, ?, ?, ?)
                            ''', (tweet_id, content, created_at, userid))
                            print(f"Tweet {tweet_id} inserted successfully")
                            inserted_tweets.append(tweet_id)

                    except Exception as e:
                        print(f"Error processing tweet {tweet_index + 1}: {str(e)}")
                        print(f"Tweet data: {json.dumps(tweet, indent=2)}")
                        error_tweets.append(tweet_id if tweet_id else "Unknown ID")

            except Exception as e:
                print(f"Error processing item {item_index + 1}: {str(e)}")
                print(f"Item data: {json.dumps(item, indent=2)}")

        conn.commit()
        conn.close()

        print(f"\nOperation completed. Inserted: {len(inserted_tweets)}, Skipped: {len(skipped_tweets)}, Errors: {len(error_tweets)}")
        return jsonify({
            "inserted": inserted_tweets,
            "skipped": skipped_tweets,
            "errors": error_tweets,
            "total_processed": len(inserted_tweets) + len(skipped_tweets) + len(error_tweets),
            "total_inserted": len(inserted_tweets),
            "total_skipped": len(skipped_tweets),
            "total_errors": len(error_tweets)
        }), 200

    except Exception as e:
        print(f"Unexpected error in add_all_tweets: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

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

# API to add or update users
@app.route('/add_users', methods=['POST'])
def add_users():
    print("add_users function called")
    try:
        data = request.json
        print("=== Received Full Data ===")
        print(json.dumps(data, indent=2))
        print("=== End of Full Data ===\n")

        if not data or 'data' not in data or 'users' not in data['data']:
            print("Invalid JSON data received")
            return jsonify({"error": "Invalid JSON data received"}), 400

        users = data['data']['users']
        if not isinstance(users, list):
            print("Invalid users data - expected a list")
            return jsonify({"error": "Invalid users data structure"}), 400

        conn = connect_db()
        cursor = conn.cursor()

        # 确保表存在
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

        inserted_users = []
        updated_users = []
        error_users = []

        for user in users:
            try:
                user_id = user.get('id')
                if not user_id:
                    raise ValueError("Missing user ID")

                # 检查用户是否存在
                cursor.execute('SELECT user_id FROM users_v2 WHERE user_id = ?', (user_id,))
                exists = cursor.fetchone()

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

                if exists:
                    # 更新现有用户
                    cursor.execute('''
                        UPDATE users_v2 
                        SET screen_name=?, name=?, description=?, location=?,
                            followers_count=?, friends_count=?, listed_count=?,
                            favourites_count=?, media_count=?, created_at=?,
                            profile_image_url=?, verified=?, last_updated=?
                        WHERE user_id=?
                    ''', user_data[1:] + (user_id,))
                    updated_users.append(user_id)
                    print(f"Updated user {user_id}")
                else:
                    # 插入新用户
                    cursor.execute('''
                        INSERT INTO users_v2 (
                            user_id, screen_name, name, description, location,
                            followers_count, friends_count, listed_count,
                            favourites_count, media_count, created_at,
                            profile_image_url, verified, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', user_data)
                    inserted_users.append(user_id)
                    print(f"Inserted user {user_id}")

            except Exception as e:
                print(f"Error processing user: {str(e)}")
                print(f"User data: {json.dumps(user, indent=2)}")
                error_users.append(str(user_id) if user_id else "Unknown ID")

        conn.commit()
        conn.close()

        return jsonify({
            "inserted": inserted_users,
            "updated": updated_users,
            "errors": error_users,
            "total_processed": len(users),
            "total_inserted": len(inserted_users),
            "total_updated": len(updated_users),
            "total_errors": len(error_users)
        }), 200

    except Exception as e:
        print(f"Unexpected error in add_users: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

@app.route('/get_user_follower_averages', methods=['GET'])
def get_user_follower_averages():
    print("get_user_follower_averages function called")
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

        # 用户粉丝数统计数据
        user_follower_averages = {}

        # 获取所有用户的粉丝数历史数据
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

        # 格式化结果
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004, debug=True)

