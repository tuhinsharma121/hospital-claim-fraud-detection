import logging
import sys

import flask
from flask import Flask, request
from flask_cors import CORS
from pyspark.sql import SparkSession
from model_platform.src.operationalization.anomaly.profile_anomaly.score import load_profile_anomaly_model, \
    get_profile_anomaly_score
from config import *
import pandas as pd


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
app = Flask(__name__)
CORS(app)

spark = SparkSession.builder.appName('MAAS').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

model_dict = dict()
model_dict["default"] = load_profile_anomaly_model(spark=spark, data_source="default")
print(model_dict)


@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/get-pas', methods=['POST'])
def calculate_pas():
    app.logger.info("Submitting the training job")

    json_req = request.get_json()
    date_source = json_req.get("data_source")

    app.logger.info("input request : {input_dict}".format(input_dict=json_req))
    data_source = "default"
    actor_type = json_req["actor_type"]
    time_window = json_req["time_window"]
    try:
        if actor_type == USER_TYPE:
            df = pd.DataFrame(
                data=[[json_req["patient_id"], json_req["maximum_claim_amount"], json_req["minimum_claim_amount"],
                       json_req["total_claim_amount"], json_req["total_claim_count"]]],
                columns=['patient_id', 'maximum_claim_amount', 'minimum_claim_amount',
                         'total_claim_amount', 'total_claim_count'])
            score = get_profile_anomaly_score(model_dict=model_dict[data_source], input_df=df, actor_type=actor_type,
                                              time_window=time_window)
        elif actor_type == ENTITY_TYPE:
            df = pd.DataFrame(
                data=[[json_req["hospital_id"], json_req["maximum_claim_amount"], json_req["maximum_patient_age"],
                       json_req["minimum_claim_amount"], json_req["minimum_patient_age"],
                       json_req["total_claim_amount"], json_req["total_claim_count"]]],
                columns=['hospital_id', 'maximum_claim_amount', 'maximum_patient_age', 'minimum_claim_amount',
                         'minimum_patient_age', 'total_claim_amount', 'total_claim_count'])
            score = get_profile_anomaly_score(model_dict=model_dict[data_source], input_df=df, actor_type=actor_type,
                                              time_window=time_window)

    except:
        score = 0.0
    app.logger.info("response : {score}".format(score=score))
    result = float(score)
    app.logger.error(result)
    return flask.jsonify(result)


if __name__ == '__main__':
    app.run()