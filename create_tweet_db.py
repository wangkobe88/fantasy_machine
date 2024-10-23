import sqlite3

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('/home/lighthouse/tweets.db')

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Original table creation and modification
create_table_query = '''
CREATE TABLE IF NOT EXISTS tweets (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Title TEXT,
    Author TEXT,
    CreateTime TEXT,
    Link TEXT,
    TweetId TEXT UNIQUE,
    Score INTEGER
);
'''

alter_table_query = '''
ALTER TABLE tweets
ADD COLUMN TweetType TEXT;
'''

# New table creation for tweets_v2
create_table_v2_query = '''
CREATE TABLE IF NOT EXISTS tweets_v2 (
    tweetID TEXT PRIMARY KEY,
    Content TEXT,
    CreatedAt TEXT
);
'''

# Execute the SQL commands
#cursor.execute(create_table_query)
#cursor.execute(alter_table_query)
cursor.execute(create_table_v2_query)

# SQL command to clear the tweets table
clear_table_query = '''
DELETE FROM tweets;
'''

# Execute the SQL command to clear the table
#cursor.execute(clear_table_query)

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print("Database tables created, modified, and cleared successfully.")
