
import json, os
from flask import Flask, request, jsonify
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET = os.getenv("BUCKET")
ASSET = os.getenv("PATH_TO_ASSET")

client = storage.Client()
bucket = client.bucket(BUCKET)
blob = bucket.blob(ASSET)

app = Flask(__name__)

@app.route('/', methods=['GET'])
def neighbourhood_query():
    
    nhood = request.args.get("neighbourhood", "Bayside")

    with blob.open(mode='r') as file_obj:
        jf = json.load(file_obj)
        for record in jf:
            if nhood in record:
                return jsonify(record[nhood])
                
        return jsonify({'error': 'neighbourhood not found'})


@app.route('/_ah/warmup')
def warmup():
    return '', 200, {}