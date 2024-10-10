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
            cursor.execute('SELECT TweetId FROM tweets WHERE TweetId = ?', (tweet['TweetId'],))
            result = cursor.fetchone()

            if result:
                skipped_tweets.append(tweet['TweetId'])
            else:
                cursor.execute('''
                    INSERT INTO tweets (Title, Author, CreateTime, Link, TweetId, Score) 
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                tweet['Title'], tweet['Author'], tweet['CreateTime'], tweet['Link'], tweet['TweetId'], tweet['Score']))
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

    conn = connect_db()
    cursor = conn.cursor()

    # Get current date in the format 'Mon Sep 23' from the CreateTime
    today = datetime.utcnow().strftime('%a %b %d')
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%a %b %d')

    try:
        # Query to fetch tweets where CreateTime starts with today's date
        cursor.execute("SELECT * FROM tweets WHERE CreateTime LIKE ?", (f'{yesterday}%',))
        rows = cursor.fetchall()

        # Transform the result into a list of dictionaries
        tweets = []
        for row in rows:
            tweet = {
                "ID": row[0],
                "Title": row[1],
                "Author": row[2],
                "CreateTime": row[3],
                "Link": row[4],
                "TweetId": row[5],
                "Score": row[6]
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
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Query to fetch the latest 50 tweets ordered by CreateTime descending
        cursor.execute("SELECT * FROM tweets ORDER BY CreateTime DESC LIMIT 50")
        rows = cursor.fetchall()

        # Transform the result into a list of dictionaries
        tweets = []
        for row in rows:
            tweet = {
                "ID": row[0],
                "Title": row[1],
                "Author": row[2],
                "CreateTime": row[3],
                "Link": row[4],
                "TweetId": row[5],
                "Score": row[6]
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

    # Get current date in the format 'Mon Sep 23' from the CreateTime
    today = datetime.utcnow().strftime('%a %b %d')
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%a %b %d')

    try:
        # Query to fetch today's tweets ordered by CreateTime
        cursor.execute("SELECT Title, Author, CreateTime, Link FROM tweets WHERE CreateTime LIKE ?", (f'{yesterday}%',))
        rows = cursor.fetchall()

        if not rows:
            return Response("No tweets found for today.", mimetype='text/plain')

        # Build a Twitter-friendly message
        tweet_text = "🔥 今日热门推文 🔥\n\n"  # A catchy header
        tweet_text += f"日期: {today}\n\n"

        for row in rows:
            title, author, create_time, link = row
            username = extract_username(link)
            tweet_text += f"📌 {title}\n👤 作者: {author} @{username}\n🕒 时间: {create_time}\n🔗 链接: {link}\n\n"
            tweet_text += "—" * 30 + "\n\n"  # Separator for readability

        # Return as plain text for easy copying and pasting
        return Response(tweet_text, mimetype='text/plain')

    except Exception as e:
        return Response(f"An error occurred: {str(e)}", mimetype='text/plain', status=500)
    finally:
        conn.close()


# API to get the total number of records ordered by CreateTime
@app.route('/get_total_tweets', methods=['GET'])
def get_total_tweets():
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Query to get the total number of tweets ordered by CreateTime
        cursor.execute("SELECT COUNT(*) FROM tweets")
        total = cursor.fetchone()[0]

        return jsonify({"total_tweets": total}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
