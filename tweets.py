from flask import Flask, request, jsonify, Response
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import traceback

app = Flask(__name__)

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

        for tweet in data:
            try:
                print(f"Processing tweet: {tweet.get('TweetId', 'Unknown ID')}")
                # Check if a tweet with the same TweetId and TweetType exists
                cursor.execute('SELECT TweetId FROM tweets WHERE TweetId = ? AND TweetType = ?', 
                               (tweet['TweetId'], tweet['TweetType']))
                result = cursor.fetchone()

                if result:
                    print(f"Tweet {tweet['TweetId']} already exists, skipping")
                    skipped_tweets.append(tweet['TweetId'])
                else:
                    cursor.execute('''
                        INSERT INTO tweets (Title, Author, CreateTime, Link, TweetId, Score, TweetType) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                    tweet['Title'], tweet['Author'], tweet['CreateTime'], tweet['Link'], 
                    tweet['TweetId'], tweet['Score'], tweet['TweetType']))
                    print(f"Tweet {tweet['TweetId']} inserted successfully")
                    inserted_tweets.append(tweet['TweetId'])
            except KeyError as ke:
                print(f"KeyError processing tweet: {ke}")
                return jsonify({"error": f"Missing key in tweet data: {ke}"}), 400
            except Exception as e:
                print(f"Error inserting tweet {tweet.get('TweetId', 'Unknown ID')}: {str(e)}")
                conn.rollback()
                return jsonify({"error": f"Error inserting tweet {tweet.get('TweetId', 'Unknown ID')}: {str(e)}"}), 500

        conn.commit()
        conn.close()

        print(f"Operation completed. Inserted: {len(inserted_tweets)}, Skipped: {len(skipped_tweets)}")
        return jsonify({"inserted": inserted_tweets, "skipped": skipped_tweets}), 200

    except Exception as e:
        print(f"Unexpected error in add_tweets: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

# API to get today's tweets
@app.route('/get_todays_tweets', methods=['GET'])
def get_todays_tweets():
    print("..get_todays_tweets")
    tweet_type = request.args.get('tweet_type')

    conn = connect_db()
    cursor = conn.cursor()

    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%a %b %d')

    try:
        if tweet_type:
            cursor.execute("SELECT * FROM tweets WHERE CreateTime LIKE ? AND TweetType = ?", (f'{yesterday}%', tweet_type))
        else:
            cursor.execute("SELECT * FROM tweets WHERE CreateTime LIKE ?", (f'{yesterday}%',))
        rows = cursor.fetchall()

        tweets = []
        for row in rows:
            tweet = {
                "ID": row[0],
                "Title": row[1],
                "Author": row[2],
                "CreateTime": row[3],
                "Link": row[4],
                "TweetId": row[5],
                "Score": row[6],
                "TweetType": row[7]
            }
            tweets.append(tweet)

        return jsonify({"tweets": tweets}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# API to get the latest 50 tweets, sorted by CreateTime
@app.route('/get_latest_tweets', methods=['GET'])
def get_latest_tweets():
    tweet_type = request.args.get('tweet_type')
    conn = connect_db()
    cursor = conn.cursor()

    try:
        if tweet_type:
            cursor.execute("SELECT * FROM tweets WHERE TweetType = ? ORDER BY CreateTime DESC LIMIT 50", (tweet_type,))
        else:
            cursor.execute("SELECT * FROM tweets ORDER BY CreateTime DESC LIMIT 50")
        rows = cursor.fetchall()

        tweets = []
        for row in rows:
            tweet = {
                "ID": row[0],
                "Title": row[1],
                "Author": row[2],
                "CreateTime": row[3],
                "Link": row[4],
                "TweetId": row[5],
                "Score": row[6],
                "TweetType": row[7]
            }
            tweets.append(tweet)

        return jsonify({"tweets": tweets}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# API to get today's tweets formatted for Twitter posting
@app.route('/get_todays_tweets_formated', methods=['GET'])
def get_todays_tweets_formated():
    tweet_type = request.args.get('tweet_type')
    if tweet_type == "meme":
        tweet_type = "Meme"
    conn = connect_db()
    cursor = conn.cursor()

    # 使用北京时间
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    today = now.strftime('%a %b %d')
    yesterday = (now - timedelta(days=1)).strftime('%a %b %d')

    # 读取 meme_kols.csv 文件
    meme_kols = {}
    with open('./data/meme_kols.csv', 'r') as f:
        next(f)  # 跳过标题行
        for line in f:
            username, influence = line.strip().split(',')[:2]
            meme_kols[username.lower()] = influence

    try:
        # 修改 SQL 查询以包含今天和昨天的推文，并包括 Score，按 CreateTime 降序排序
        if tweet_type:
            cursor.execute("SELECT Title, Author, CreateTime, Link, TweetType, Score FROM tweets WHERE (CreateTime LIKE ? OR CreateTime LIKE ?) AND TweetType = ? ORDER BY CreateTime DESC", (f'{today}%', f'{yesterday}%', tweet_type))
        else:
            cursor.execute("SELECT Title, Author, CreateTime, Link, TweetType, Score FROM tweets WHERE CreateTime LIKE ? OR CreateTime LIKE ? ORDER BY CreateTime DESC", (f'{today}%', f'{yesterday}%'))
        rows = cursor.fetchall()

        if not rows:
            return Response("今天或昨天没有找到推文。", mimetype='text/html')

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
                .tweet-title {{
                    font-size: 18px;
                    font-weight: bold;
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

        for row in rows:
            title, author, create_time, link, tweet_type, score = row
            username = extract_username(link)
            influence = meme_kols.get(username.lower(), "未知") if username else "未知"
            
            create_time_obj = datetime.strptime(create_time, "%a %b %d %H:%M:%S %z %Y")
            create_time_obj = create_time_obj.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Shanghai"))
            create_time_cn = create_time_obj.strftime("%Y年%m月%d日 %H:%M:%S")
            
            html_content += f"""
            <div class="tweet">
                <div class="tweet-title">{title}</div>
                <div class="tweet-info">
                    <p>👤 作者: {author} @{username}</p>
                    <p>🕒 时间: {create_time_cn}</p>
                    <p>🔗 链接: <a href="{link}" target="_blank" class="tweet-link">{link}</a></p>
                    <p>📊 类型: {tweet_type}</p>
                    <p>💯 评分: {score}</p>
                    <p>🌟 影响力: {influence}</p>
                </div>
            </div>
            """

        html_content += """
        </body>
        </html>
        """

        return Response(html_content, mimetype='text/html')

    except Exception as e:
        return Response(f"<h1>发生错误</h1><p>{str(e)}</p>", mimetype='text/html', status=500)
    finally:
        conn.close()


# API to get the total number of records ordered by CreateTime
@app.route('/get_total_tweets', methods=['GET'])
def get_total_tweets():
    tweet_type = request.args.get('tweet_type')
    conn = connect_db()
    cursor = conn.cursor()

    try:
        if tweet_type:
            cursor.execute("SELECT COUNT(*) FROM tweets WHERE TweetType = ?", (tweet_type,))
        else:
            cursor.execute("SELECT COUNT(*) FROM tweets")
        total = cursor.fetchone()[0]

        return jsonify({"total_tweets": total}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# API to add all tweets
@app.route('/add_all_tweets', methods=['POST'])
def add_all_tweets():
    print("Entering add_all_tweets function")
    try:
        data = request.json
        if not data or 'output' not in data:
            print("Invalid data format received")
            return jsonify({"error": "Invalid data format", "details": "Expected 'output' key in JSON data"}), 400

        tweets = []
        for output in data['output']:
            try:
                tweets.extend(json.loads(output['output']))
            except json.JSONDecodeError as jde:
                print(f"JSON decode error: {str(jde)}")
                return jsonify({"error": "JSON decode error", "details": str(jde)}), 400

        print(f"Total tweets received: {len(tweets)}")

        conn = connect_db()
        cursor = conn.cursor()

        inserted_count = 0
        skipped_count = 0
        error_count = 0
        error_details = []

        for index, tweet in enumerate(tweets):
            try:
                print(f"Processing tweet {index + 1}/{len(tweets)}: {tweet.get('TweetId', 'Unknown ID')}")
                
                # Check if tweet already exists
                cursor.execute("SELECT TweetId FROM tweets WHERE TweetId = ?", (tweet['TweetId'],))
                if cursor.fetchone():
                    print(f"Tweet {tweet['TweetId']} already exists, skipping")
                    skipped_count += 1
                    continue

                # Convert CreateTime to datetime object
                create_time = datetime.strptime(tweet['CreateTime'], '%a %b %d %H:%M:%S %z %Y')
                
                # Insert tweet into database
                cursor.execute("""
                    INSERT INTO tweets (Title, Author, CreateTime, Link, TweetId, TweetType, Score)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    tweet['Title'],
                    tweet['Author'],
                    create_time,
                    tweet['Link'],
                    tweet['TweetId'],
                    tweet['TweetType'],
                    tweet['Score']
                ))
                inserted_count += 1
                print(f"Inserted tweet {tweet['TweetId']}")
            except sqlite3.IntegrityError as ie:
                print(f"IntegrityError: Tweet {tweet['TweetId']} already exists")
                skipped_count += 1
                error_details.append(f"IntegrityError for tweet {tweet['TweetId']}: {str(ie)}")
            except KeyError as ke:
                print(f"KeyError processing tweet: {ke}")
                error_count += 1
                error_details.append(f"KeyError for tweet {tweet.get('TweetId', 'Unknown')}: Missing key {str(ke)}")
            except Exception as e:
                print(f"Error processing tweet {tweet.get('TweetId', 'Unknown')}: {str(e)}")
                error_count += 1
                error_details.append(f"Error for tweet {tweet.get('TweetId', 'Unknown')}: {str(e)}")

        conn.commit()
        print(f"Operation completed. Inserted: {inserted_count}, Skipped: {skipped_count}, Errors: {error_count}")
        return jsonify({
            "message": f"Operation completed",
            "inserted": inserted_count,
            "skipped": skipped_count,
            "errors": error_count,
            "error_details": error_details
        }), 200
    except Exception as e:
        print(f"Unexpected error in add_all_tweets: {str(e)}")
        traceback.print_exc()  # This will print the full traceback
        return jsonify({
            "error": "Unexpected error",
            "details": str(e),
            "traceback": traceback.format_exc()
        }), 500
    finally:
        if 'conn' in locals():
            conn.close()
        print("Exiting add_all_tweets function")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
