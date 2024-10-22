import sqlite3

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('/home/lighthouse/tweets.db')

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# SQL command to create a table with 7 fields
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

# Execute the SQL command to create the table
#cursor.execute(create_table_query)

# SQL command to add a new column TweetType
alter_table_query = '''
ALTER TABLE tweets
ADD COLUMN TweetType TEXT;
'''

# Execute the SQL command to add the new column
cursor.execute(alter_table_query)

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print("Database table created and modified successfully.")
