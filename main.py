import pandas as pd
from flask import Flask, request 
import lib_aws as aws
import json


app = Flask(__name__)

# create service boto3
session = aws.create_session()

# download file from aws
aws.download_from_aws(session,bucket='project-finance-api', file='fundamentus.parquet' )
data = pd.read_parquet('fundamentus.parquet').reset_index()

@app.route('/', methods=["GET"])

def homepage():
    return json.dumps(data.to_dict(orient='records'), indent=4), 200

@app.route("/get_data", methods=["GET"])

def fetchdata():
    ticket = request.args['ticket']
    return data[data['papel'] == ticket.upper()].to_json(), 200


# run api
app.run(host='0.0.0.0')

# test = http://127.0.0.1:5000/get_data?ticket=MGLU3

