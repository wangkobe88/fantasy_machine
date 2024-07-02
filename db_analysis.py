import sqlite3

def get_table_info(database, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # Query to get table schema
    query = f"PRAGMA table_info({table_name})"
    cursor.execute(query)

    # Fetch all rows from the query result
    columns_info = cursor.fetchall()

    # Close the connection
    conn.close()

    # Print table schema
    print(f"Schema of the table '{table_name}':")
    for column in columns_info:
        cid, name, type_, notnull, dflt_value, pk = column
        print(f"Column ID: {cid}, Name: {name}, Type: {type_}, Not Null: {notnull}, Default Value: {dflt_value}, Primary Key: {pk}")

if __name__ == "__main__":
    database = "/home/lighthouse/count.db"  # Replace with your SQLite database file
    table_name = "count"  # Replace with your table name
    get_table_info(database, table_name)
