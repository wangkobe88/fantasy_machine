from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Load the CSV file when the server starts
data = pd.read_csv('./data/og.csv')

# Function to get unique values and their counts for a given column
def get_unique_values_and_counts(data, column):
    return data[column].value_counts().to_dict()

# Function to query data based on conditions
def query_data(data, conditions):
    query_str = " & ".join([f"`{col}` {op} '{val}'" for col, op, val in conditions])
    return data.query(query_str).to_dict(orient='records')

# Endpoint to get unique values and their counts
@app.route('/unique_values', methods=['GET'])
def unique_values():
    column = request.args.get('column')
    if not column:
        return jsonify({"error": "Column parameter is required"}), 400
    result = get_unique_values_and_counts(data, column)
    return jsonify(result)

# Endpoint to query data
@app.route('/query', methods=['POST'])
def query():
    conditions = request.json.get('conditions')
    if not conditions:
        return jsonify({"error": "Conditions are required"}), 400
    result = query_data(data, conditions)
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
