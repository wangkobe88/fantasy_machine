from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)


# Database connection
def connect_db():
    conn = sqlite3.connect('/home/lighthouse/tweets.db')
    return conn


# API to insert multiple tweets into the database
@app.route('/add_tweets', methods=['POST'])
def add_tweets():
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
