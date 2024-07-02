from flask import Flask, request, jsonify
import pandas as pd
import json

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
    print(request)
    conditions = request.json.get('conditions')
    print(conditions)
    if not conditions:
        return jsonify({"error": "Conditions are required"}), 400
    conditions = json.loads(conditions)
    result = query_data(data, conditions)
    return jsonify(result)

# Function to query and sort wallets by occurrence
def query_and_sort_wallets(data, conditions):
    query_str = " & ".join([f"`{col}` {op} '{val}'" for col, op, val in conditions])
    queried_data = data.query(query_str)
    sorted_wallets = queried_data['wallet'].value_counts().sort_values(ascending=False).to_dict()
    return sorted_wallets

# Endpoint to query and sort wallets by occurrence
@app.route('/query_and_sort_wallets', methods=['POST'])
def query_sort_wallets():
    try:
        conditions_str = request.json.get('conditions')
        if not conditions_str:
            return jsonify({"error": "Conditions are required"}), 400

        # Parse conditions from string to list
        conditions = json.loads(conditions_str)

        result = query_and_sort_wallets(data, conditions)
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": f"Invalid conditions format: {str(e)}"}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
