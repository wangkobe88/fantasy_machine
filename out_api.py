from flask import Flask, request, jsonify, redirect
from flasgger import Swagger
from index_db import *
from util import *

app = Flask(__name__)
swagger = Swagger(app)

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

def get_pfp_from_csv_all(num):
    if num in All_dict:
        imageurl = All_dict[num]
    return imageurl
@app.route('/root/<inscription_id>')
def root(inscription_id):
    result = query_data('count.db','count', f"inscription = '{inscription_id}'")
    return jsonify(result[0])

@app.route('/root/PFP/inscriptions/<inscription_id>')
def root_pfp_inscriptions(inscription_id):
    result = query_data('count.db','count', f"inscription = '{inscription_id}'")
    if result[0][6] != None:
        original_image_url = result[0][6]
    else:
        original_image_url = get_pfp_from_csv_all(result[0][2])
    return redirect(original_image_url)

@app.route('/root/PFP/nums/<num>')
def root_pfp_nums(num):
    result = query_data('count.db','count', f"number = '{num}'")
    if result[0][6] != None:
        original_image_url = result[0][6]
    else:
        original_image_url = get_pfp_from_csv_all(result[0][2])
    return redirect(original_image_url)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
