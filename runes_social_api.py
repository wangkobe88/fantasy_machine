from flask import Flask, request, jsonify
import csv

app = Flask(__name__)

def load_runes_social_data():
    data = []
    with open('./data/runes_social.csv', mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            # 清理空值
            cleaned_row = {k: v.strip() if v else "" for k, v in row.items()}
            data.append(cleaned_row)
    return data

# 加载数据
RUNES_DATA = load_runes_social_data()

@app.route('/distinct_values', methods=['GET'])
def distinct_values():
    column = request.args.get('column')
    if not column:
        return jsonify({"error": "No column specified"}), 400
    
    # 获取指定列的所有非空值
    values = set(row[column] for row in RUNES_DATA if row.get(column))
    return jsonify(list(values))

@app.route('/query', methods=['GET'])
def query():
    # 获取查询参数
    name = request.args.get('Name', '')
    telegram = request.args.get('Telegram', '')
    x = request.args.get('X', '')
    dc = request.args.get('DC', '')
    
    results = RUNES_DATA
    
    # 根据提供的参数进行过滤
    if name:
        results = [r for r in results if name.lower() in r['Name'].lower()]
    if telegram:
        results = [r for r in results if telegram.lower() in r['Telegram'].lower()]
    if x:
        results = [r for r in results if x.lower() in r['X'].lower()]
    if dc:
        results = [r for r in results if dc.lower() in r['DC'].lower()]
    
    return jsonify({
        "total_count": len(results),
        "data": results
    })

@app.route('/social_info', methods=['GET'])
def social_info():
    name = request.args.get('name')
    if not name:
        return jsonify({"error": "No name specified"}), 400
    
    # 查找匹配的记录
    for record in RUNES_DATA:
        if record['Name'].lower() == name.lower():
            return jsonify(record)
    
    return jsonify({"error": "Name not found"}), 404

@app.route('/column_data', methods=['GET'])
def column_data():
    column = request.args.get('column')
    if not column:
        return jsonify({"error": "No column specified"}), 400
    
    if column not in RUNES_DATA[0].keys():
        return jsonify({"error": f"Column '{column}' not found"}), 404
    
    # 获取指定列的所有非空值，并保持原始顺序
    values = [row[column] for row in RUNES_DATA if row[column]]
    
    # 如果是Name列，添加特殊处理
    if column == 'Name':
        # 获取原始值
        original_values = values.copy()
        # 处理后的值（将•替换为空格）
        processed_values = [name.replace('•', ' ').strip() for name in original_values]
        # 合并原始值和处理后的值
        values = original_values + processed_values
    
    return jsonify({
        "column": column,
        "total_count": len(values),
        "data": values
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5008) 