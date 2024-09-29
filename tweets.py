from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime

app = Flask(__name__)


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

    try:
        # Query to fetch tweets where CreateTime starts with today's date
        cursor.execute("SELECT * FROM tweets WHERE CreateTime LIKE ?", (f'{today}%',))
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
