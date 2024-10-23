from flask import Flask, request, jsonify, Response
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json

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
                content = json.dumps(tweet)  # Convert the entire tweet object to a JSON string
                created_at = tweet.get('created_at')

                print(f"Tweet ID: {tweet_id}, Created At: {created_at}")

                # Check if a tweet with the same tweetID exists
                cursor.execute('SELECT tweetID FROM tweets_v2 WHERE tweetID = ?', (tweet_id,))
                result = cursor.fetchone()

                if result:
                    print(f"Tweet {tweet_id} already exists, skipping")
                    skipped_tweets.append(tweet_id)
                else:
                    cursor.execute('''
                        INSERT INTO tweets_v2 (tweetID, Content, CreatedAt) 
                        VALUES (?, ?, ?)
                    ''', (tweet_id, content, created_at))
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
    conn = connect_db()
    cursor = conn.cursor()

    # 使用北京时间
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    today = now.strftime('%Y-%m-%d')
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        # 修改 SQL 查询以包含今天和昨天的推文，按 CreatedAt 降序排序
        cursor.execute("SELECT Content FROM tweets_v2 WHERE date(CreatedAt) IN (?, ?) ORDER BY CreatedAt DESC", (today, yesterday))
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

        for row in rows:
            tweet_data = json.loads(row[0])
            full_text = tweet_data.get('full_text', '')
            name = tweet_data['user']['name']
            screen_name = tweet_data['user']['screen_name']
            created_at = tweet_data['created_at']
            tweet_id = tweet_data['rest_id']
            
            create_time_obj = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            create_time_obj = create_time_obj.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Shanghai"))
            create_time_cn = create_time_obj.strftime("%Y年%m月%d日 %H:%M:%S")
            
            link = f"https://twitter.com/{screen_name}/status/{tweet_id}"
            
            html_content += f"""
            <div class="tweet">
                <div class="tweet-text">{full_text}</div>
                <div class="tweet-info">
                    <p>👤 作者: {name} @{screen_name}</p>
                    <p>🕒 时间: {create_time_cn}</p>
                    <p>🔗 链接: <a href="{link}" target="_blank" class="tweet-link">{link}</a></p>
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


# API to get tweets from the last N days
@app.route('/get_tweets', methods=['GET'])
def get_tweets():
    try:
        days = int(request.args.get('days', 1))
        conn = connect_db()
        cursor = conn.cursor()

        # Calculate the date N days ago
        n_days_ago = (datetime.now(ZoneInfo("UTC")) - timedelta(days=days)).isoformat()

        cursor.execute("SELECT Content FROM tweets_v2 WHERE CreatedAt > ?", (n_days_ago,))
        rows = cursor.fetchall()

        processed_tweets = []
        for row in rows:
            tweet_data = json.loads(row[0])
            
            # Process the tweet data
            processed_tweet = {
                "created_at": tweet_data.get("created_at"),
                "favorite_count": tweet_data.get("favorite_count"),
                "full_text": tweet_data.get("full_text"),
                "rest_id": tweet_data.get("rest_id"),
                "retweet_count": tweet_data.get("retweet_count"),
                "user": tweet_data.get("user"),
                "Link": f"https://twitter.com/{tweet_data['user']['screen_name']}/status/{tweet_data['rest_id']}"
            }
            
            processed_tweets.append(processed_tweet)

        return jsonify({"tweets": processed_tweets}), 200

    except ValueError:
        return jsonify({"error": "Invalid 'days' parameter. Must be an integer."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# API to add all tweets
@app.route('/add_all_tweets', methods=['POST'])
def add_all_tweets():
    print("add_all_tweets function called")
    try:
        data = request.json
        if not data or 'output' not in data:
            print("Invalid JSON data received")
            return jsonify({"error": "Invalid JSON data received"}), 400

        conn = connect_db()
        cursor = conn.cursor()

        inserted_tweets = []
        skipped_tweets = []
        error_tweets = []

        for item in data['output']:
            if 'data' in item and 'freeBusy' in item['data'] and 'post' in item['data']['freeBusy']:
                tweets = item['data']['freeBusy']['post']
                for tweet in tweets:
                    try:
                        tweet_id = tweet.get('rest_id')
                        if not tweet_id:
                            raise ValueError("Missing tweet ID")

                        content = json.dumps(tweet)
                        created_at = tweet.get('created_at')

                        # Check if a tweet with the same tweetID exists
                        cursor.execute('SELECT tweetID FROM tweets_v2 WHERE tweetID = ?', (tweet_id,))
                        result = cursor.fetchone()

                        if result:
                            print(f"Tweet {tweet_id} already exists, skipping")
                            skipped_tweets.append(tweet_id)
                        else:
                            cursor.execute('''
                                INSERT INTO tweets_v2 (tweetID, Content, CreatedAt) 
                                VALUES (?, ?, ?)
                            ''', (tweet_id, content, created_at))
                            print(f"Tweet {tweet_id} inserted successfully")
                            inserted_tweets.append(tweet_id)

                    except Exception as e:
                        print(f"Error processing tweet: {str(e)}")
                        error_tweets.append(tweet_id if tweet_id else "Unknown ID")

        conn.commit()
        conn.close()

        print(f"Operation completed. Inserted: {len(inserted_tweets)}, Skipped: {len(skipped_tweets)}, Errors: {len(error_tweets)}")
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
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004, debug=True)
