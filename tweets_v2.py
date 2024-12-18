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

    # 使用 UTC 时间
    now = datetime.now(ZoneInfo("UTC"))
    two_days_ago = (now - timedelta(hours=48))
    print(f"Querying for tweets from {two_days_ago.strftime('%Y-%m-%d %H:%M:%S %z')} to {now.strftime('%Y-%m-%d %H:%M:%S %z')}")
    
    # 读取 meme_kols.csv 文件
    meme_kols = {}
    try:
        with open('./data/meme_kols.csv', 'r') as f:
            next(f)  # 跳过标题行
            for line in f:
                username, influence = line.strip().split(',')[:2]
                meme_kols[username.lower()] = influence
        print(f"Loaded {len(meme_kols)} KOL records")
    except Exception as e:
        print(f"Error reading meme_kols.csv: {e}")
        meme_kols = {}  # 如果文件读取失败，使用空字典

    try:
        # 首先检查数据库中是否有数据
        cursor.execute("SELECT COUNT(*) FROM tweets_v2")
        total_count = cursor.fetchone()[0]
        print(f"Total tweets in database: {total_count}")

        # 修改查询语句，先获取最新的推文进行检查
        check_query = """
        SELECT Content, CreatedAt, keywords FROM tweets_v2 
        ORDER BY rowid DESC
        LIMIT 5
        """
        cursor.execute(check_query)
        check_rows = cursor.fetchall()
        
        print("\nChecking most recent tweets by rowid:")
        for i, row in enumerate(check_rows):
            content = json.loads(row[0])
            created_at = row[1]
            keywords = row[2]
            print(f"Tweet {i+1}: Created at {created_at}, Keywords: {keywords}")

        # 主查询
        query = """
        SELECT Content, CreatedAt, keywords FROM tweets_v2 
        WHERE CreatedAt IS NOT NULL
        ORDER BY rowid DESC
        LIMIT 500
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"\nFetched {len(rows)} tweets for processing")

        # 修改日期过滤逻辑
        filtered_tweets = []
        for row in rows:
            try:
                content = json.loads(row[0])
                created_at = row[1]
                keywords = row[2] or "未知"
                content['keywords'] = keywords

                # 解析日期并进行比较
                tweet_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                tweet_date_utc = tweet_date.astimezone(ZoneInfo("UTC"))

                print(f"\nProcessing tweet date: {tweet_date_utc}")
                print(f"Comparing with range: {two_days_ago} to {now}")

                if two_days_ago <= tweet_date_utc <= now:
                    filtered_tweets.append(content)
                    print("Tweet included")
                else:
                    print("Tweet excluded")

            except Exception as e:
                print(f"Error processing row: {e}")
                continue

        print(f"\nFiltered to {len(filtered_tweets)} tweets within last 48 hours")

        # 如果没有找到推文，添加更多调试信息
        if not filtered_tweets:
            print("\nNo tweets found within last 48 hours")
            print(f"Current time (UTC): {now}")
            print(f"Two days ago (UTC): {two_days_ago}")
            
            # 显示最近5条推文的日期
            print("\nMost recent 5 tweets dates:")
            for i, row in enumerate(rows[:5]):
                content = json.loads(row[0])
                created_at = row[1]
                print(f"Tweet {i+1}: {created_at}")

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
            full_text = full_text.encode().decode('unicode_escape')
            
            name = tweet_data['user']['name']
            name = name.encode().decode('unicode_escape')
            
            screen_name = tweet_data['user']['screen_name']
            created_at = tweet_data['created_at']
            tweet_id = tweet_data['rest_id']
            keywords = tweet_data.get('keywords', '未知')  # 获取 keywords
            
            create_time_obj = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            create_time_obj = create_time_obj.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Shanghai"))
            create_time_cn = create_time_obj.strftime("%Y年%m月%d日 %H:%M:%S")
            
            link = f"https://twitter.com/{screen_name}/status/{tweet_id}"
            
            try:
                influence = meme_kols.get(screen_name.lower(), "未知")
                influence_level = get_influence_level(influence)
            except Exception as e:
                print(f"Error getting influence level for {screen_name}: {e}")
                influence_level = "未知"
            
            html_content += f"""
            <div class="tweet">
                <div class="tweet-text">{full_text}</div>
                <div class="tweet-info">
                    <p>👤 作者: {name} @{screen_name}</p>
                    <p>🕒 时间: {create_time_cn}</p>
                    <p>🔍 检索词: {keywords}</p>
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
        
        # 带时间戳的文件名
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
        rune_names = data.get('rune_names', [])  # 获取检索词列表
        
        if not isinstance(output_list, list) or not isinstance(rune_names, list):
            print("Invalid data structure - expected lists for output and rune_names")
            return jsonify({"error": "Invalid data structure"}), 400

        conn = connect_db()
        cursor = conn.cursor()

        inserted_tweets = []
        skipped_tweets = []
        error_tweets = []
        
        # 遍历输出列表中的每个项
        for item_index, (item, keyword) in enumerate(zip(output_list, rune_names)):
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
                print(f"Using keyword: {keyword}")

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
                        print(f"Tweet ID: {tweet_id}, Created At: {created_at}, User ID: {userid}, Keyword: {keyword}")

                        # 检查推文是否存在并获取其keywords值
                        cursor.execute('SELECT keywords FROM tweets_v2 WHERE tweetID = ?', (tweet_id,))
                        result = cursor.fetchone()

                        if result:
                            existing_keywords = result[0]
                            if existing_keywords is None or existing_keywords.strip() == '':
                                # 如果推文存在但keywords为空，更新keywords
                                print(f"Tweet {tweet_id} exists with empty keywords, updating...")
                                cursor.execute('''
                                    UPDATE tweets_v2 
                                    SET keywords = ? 
                                    WHERE tweetID = ?
                                ''', (keyword, tweet_id))
                                print(f"Updated keywords for tweet {tweet_id}")
                                skipped_tweets.append(f"{tweet_id} (keywords updated)")
                            else:
                                print(f"Tweet {tweet_id} already exists with keywords: {existing_keywords}, skipping")
                                skipped_tweets.append(tweet_id)
                        else:
                            # 如果推文不存在，插入新记录
                            cursor.execute('''
                                INSERT INTO tweets_v2 (tweetID, Content, CreatedAt, userid, keywords) 
                                VALUES (?, ?, ?, ?, ?)
                            ''', (tweet_id, content, created_at, userid, keyword))
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


def normalize_keyword(keyword):
    """标准化关键词：将 • 替换为空格"""
    if keyword:
        return keyword.replace('•', ' ')
    return keyword

@app.route('/analyze_keywords', methods=['GET'])
def analyze_keywords():
    conn = connect_db()
    cursor = conn.cursor()
    
    # 使用 UTC 时间
    now = datetime.now(ZoneInfo("UTC"))
    
    # 定义时间范围
    time_ranges = {
        '3d': now - timedelta(days=3),
        '7d': now - timedelta(days=7),
        '15d': now - timedelta(days=15),
        '30d': now - timedelta(days=30),
        '90d': now - timedelta(days=90)
    }
    
    try:
        # 获取所有推文数据
        query = """
        SELECT Content, CreatedAt, keywords 
        FROM tweets_v2 
        WHERE keywords IS NOT NULL 
        AND CreatedAt IS NOT NULL
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # 初始化结果字典
        stats = {}
        
        # 处理每条推文
        for row in rows:
            try:
                content = json.loads(row[0])
                created_at = row[1]
                keyword = normalize_keyword(row[2])
                
                if not keyword:
                    continue
                
                # 初始化关键词统计
                if keyword not in stats:
                    stats[keyword] = {
                        '3d': {'posts': 0, 'likes': 0},
                        '7d': {'posts': 0, 'likes': 0},
                        '15d': {'posts': 0, 'likes': 0},
                        '30d': {'posts': 0, 'likes': 0},
                        '90d': {'posts': 0, 'likes': 0}
                    }
                
                # 解析推文日期
                tweet_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                tweet_date_utc = tweet_date.astimezone(ZoneInfo("UTC"))
                
                # 获取点赞数
                favorite_count = int(content.get('favorite_count', 0))
                
                # 更新各时间段的统计数据
                for period, start_date in time_ranges.items():
                    if tweet_date_utc >= start_date:
                        stats[keyword][period]['posts'] += 1
                        stats[keyword][period]['likes'] += favorite_count
                
            except Exception as e:
                print(f"Error processing row: {str(e)}")
                continue
        
        # 格式化结果
        formatted_results = []
        for keyword, periods in stats.items():
            result = {
                'keyword': keyword,
                'statistics': {
                    '3_days': {
                        'post_count': periods['3d']['posts'],
                        'total_likes': periods['3d']['likes']
                    },
                    '7_days': {
                        'post_count': periods['7d']['posts'],
                        'total_likes': periods['7d']['likes']
                    },
                    '15_days': {
                        'post_count': periods['15d']['posts'],
                        'total_likes': periods['15d']['likes']
                    },
                    '30_days': {
                        'post_count': periods['30d']['posts'],
                        'total_likes': periods['30d']['likes']
                    },
                    '90_days': {
                        'post_count': periods['90d']['posts'],
                        'total_likes': periods['90d']['likes']
                    }
                }
            }
            formatted_results.append(result)
        
        # 按90天内的发帖量降序排序
        formatted_results.sort(
            key=lambda x: (
                x['statistics']['90_days']['post_count'],
                x['statistics']['90_days']['total_likes']
            ),
            reverse=True
        )
        
        return jsonify({
            'updated_at': now.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'total_keywords': len(formatted_results),
            'results': formatted_results
        }), 200
        
    except Exception as e:
        print(f"Error in analyze_keywords: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return jsonify({
            'error': str(e),
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S UTC')
        }), 500
    finally:
        conn.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004, debug=True)

