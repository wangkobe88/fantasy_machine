from flask import Flask, jsonify
import csv

app = Flask(__name__)

# 在应用启动时加载CSV文件中的userid数据
userids = []

def load_userids_from_csv(file_path):
    global userids
    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        userids = [row['userid'] for row in reader]  # 读取第一列userid

# API接口，返回缓存的userid数据
@app.route('/userids', methods=['GET'])
def get_userids():
    return jsonify({'userids': userids})  # 直接返回已加载的userid

if __name__ == '__main__':
    file_path = './data/kols.csv'  # 指定你的CSV文件路径
    load_userids_from_csv(file_path)  # 在程序启动时加载CSV数据
    app.run(host='0.0.0.0', port=5002)
