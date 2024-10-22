from flask import Flask, jsonify
import csv

app = Flask(__name__)

# 在应用启动时加载CSV文件中的userid数据
userids = []
meme_kol_userids = []

def load_userids_from_csv(file_path, target_list):
    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        target_list.extend([row['userid'] for row in reader])  # 读取第一列userid

# API接口，返回缓存的userid数据
@app.route('/userids', methods=['GET'])
def get_userids():
    return jsonify({'userids': userids})  # 直接返回已加载的userid

# 新增API接口，返回meme_kols的userid数据
@app.route('/meme_kol_userids', methods=['GET'])
def get_meme_kol_userids():
    return jsonify({'meme_kol_userids': meme_kol_userids})  # 返回meme_kols的userid

if __name__ == '__main__':
    kols_file_path = './data/kols.csv'  # 指定kols.csv文件路径
    meme_kols_file_path = './data/meme_kols.csv'  # 指定meme_kols.csv文件路径
    load_userids_from_csv(kols_file_path, userids)  # 加载kols.csv数据
    load_userids_from_csv(meme_kols_file_path, meme_kol_userids)  # 加载meme_kols.csv数据
    app.run(host='0.0.0.0', port=5002)
