from flask import Flask, request, jsonify, Response
import sqlite3
from datetime import datetime, timedelta

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
    print("add tweets")
    data = request.json
    conn = connect_db()
    cursor = conn.cursor()

    inserted_tweets = []
    skipped_tweets = []

    for tweet in data:
        try:
            # Check if a tweet with the same TweetId and TweetType exists
            cursor.execute('SELECT TweetId FROM tweets WHERE TweetId = ? AND TweetType = ?', 
                           (tweet['TweetId'], tweet['TweetType']))
            result = cursor.fetchone()

            if result:
                skipped_tweets.append(tweet['TweetId'])
            else:
                cursor.execute('''
                    INSERT INTO tweets (Title, Author, CreateTime, Link, TweetId, Score, TweetType) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                tweet['Title'], tweet['Author'], tweet['CreateTime'], tweet['Link'], 
                tweet['TweetId'], tweet['Score'], tweet['TweetType']))
                inserted_tweets.append(tweet['TweetId'])
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    conn.commit()
    conn.close()

    return jsonify({"inserted": inserted_tweets, "skipped": skipped_tweets}), 200

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
    conn = connect_db()
    cursor = conn.cursor()

    # 使用 UTC 时间
    now = datetime.utcnow()
    today = now.strftime('%a %b %d')
    yesterday = (now - timedelta(days=1)).strftime('%a %b %d')

    try:
        # 修改 SQL 查询以包含今天和昨天的推文
        if tweet_type:
            cursor.execute("SELECT Title, Author, CreateTime, Link, TweetType FROM tweets WHERE (CreateTime LIKE ? OR CreateTime LIKE ?) AND TweetType = ? ORDER BY CreateTime DESC", (f'{today}%', f'{yesterday}%', tweet_type))
        else:
            cursor.execute("SELECT Title, Author, CreateTime, Link, TweetType FROM tweets WHERE CreateTime LIKE ? OR CreateTime LIKE ? ORDER BY CreateTime DESC", (f'{today}%', f'{yesterday}%'))
        rows = cursor.fetchall()

        if not rows:
            return Response("No tweets found for today or yesterday.", mimetype='text/plain')

        tweet_text = "🔥 最新热门推文 🔥\n\n"
        tweet_text += f"更新时间: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"

        for row in rows:
            title, author, create_time, link, tweet_type = row
            username = extract_username(link)
            tweet_text += f"📌 {title}\n👤 作者: {author} @{username}\n🕒 时间: {create_time}\n🔗 链接: {link}\n📊 类型: {tweet_type}\n\n"
            tweet_text += "—" * 30 + "\n\n"

        return Response(tweet_text, mimetype='text/plain')

    except Exception as e:
        return Response(f"An error occurred: {str(e)}", mimetype='text/plain', status=500)
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
