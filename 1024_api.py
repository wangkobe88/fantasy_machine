from flask import Flask, request, jsonify
import sqlite3
import csv

app = Flask(__name__)
DATABASE = '/home/lighthouse/count.db'  # 替换为你的SQLite数据库文件路径

def csv_to_dict(filename):
    result_dict = {}
    with open(filename, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            number = row['number']
            url = row['url']
            result_dict[number] = url
    return result_dict

dictionary_dict = {}
All_dict = csv_to_dict('./data/pfp_rootverse.csv')

race_subrace_dict = {
    "Cultivator": "Human", "Techie":"Human", "Warrior":"Human", "Mage":"Human",
    "Winged":"Humanoid", "Nightshade":"Humanoid", "Pyronians":"Humanoid", "Frostborn":"Humanoid",
    "DragonBlood":"Humanoid", "Abyssian":"Humanoid", "StarTraveler":"Humanoid", "Thunder":"Humanoid",
    "Forestian":"Humanoid", "SoulBinder":"Humanoid",
    "Succubus":"Demon", "Asura":"Demon",
    "Zombie":"UnderWorld", "Skeleton":"UnderWorld",
    "MaleGod":"Deity", "Goddess":"Deity", "DivineWarrior":"Deity", "Loong":"DivineBeast"
}
def get_pfp_from_csv_all(num):
    imageurl = ""
    if num in All_dict:
        imageurl = All_dict[num]
    return imageurl

def get_race(subrace):
    race = ""
    if subrace in race_subrace_dict:
        race = race_subrace_dict[subrace]
    return race

def query_db(query, args=(), one=False):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    con.close()
    return (rv[0] if rv else None) if one else rv


@app.route('/distinct_values', methods=['GET'])
def distinct_values():
    column = request.args.get('column')
    if not column:
        return jsonify({"error": "No column specified"}), 400

    query = f"SELECT {column}, COUNT(*) FROM count GROUP BY {column}"
    results = query_db(query)

    response = {row[0]: row[1] for row in results}
    return jsonify(response)


@app.route('/query', methods=['GET'])
def query():
    conditions = request.args.get('conditions')
    if not conditions:
        return jsonify({"error": "No conditions specified"}), 400

    query = f"SELECT * FROM count WHERE {conditions}"
    results = query_db(query)

    response = [
        {
            "number": row[0],
            "count": row[1],
            "race": get_race(row[2]),
            "subrace": row[2],
            "inscription": row[3],
            "wallet": row[4],
            "content": row[5],
            "url":get_pfp_from_csv_all(row[0])
        }
        for row in results
    ]
    return jsonify(response)


@app.route('/wallet_count', methods=['GET'])
def wallet_count():
    conditions = request.args.get('conditions')
    if not conditions:
        return jsonify({"error": "No conditions specified"}), 400

    query = f"SELECT wallet, COUNT(*) as wallet_count FROM count WHERE {conditions} GROUP BY wallet ORDER BY wallet_count DESC"
    results = query_db(query)

    response = [{"wallet": row[0], "count": row[1]} for row in results]
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
