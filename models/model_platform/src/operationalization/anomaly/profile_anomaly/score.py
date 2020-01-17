import argparse
import json

import pandas as pd
from pyspark.sql import SparkSession

from config import *
from model_platform.src.model.anomaly.profile_model.isolation_forest_profile_model import IsolationForestProfileModel
from utils.data_store.hdfs_data_store import HDFSDataStore
from utils.logging.pylogger import get_log_path, configure_logger


def load_sklearn_isolationforest_profile_anomaly_model_for_actor_type(spark, data_source, actor_type, time_window):
    model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                   entity_type=actor_type, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                   time_window=time_window,
                                                   model_name=SKLEARN_ISOLATION_FOREST_MODEL)
    model = IsolationForestProfileModel.load(spark=spark, path=model_path)
    return model


def load_profile_anomaly_model_for_actor_type(spark, data_source, actor_type):
    model_dict = dict()
    for time_window in TIME_WINDOW_LIST:
        model_dict[time_window] = load_sklearn_isolationforest_profile_anomaly_model_for_actor_type(spark=spark,
                                                                                                    data_source=data_source,
                                                                                                    actor_type=actor_type,
                                                                                                    time_window=time_window)
    return model_dict


def load_profile_anomaly_model(spark, data_source):
    model_dict = dict()
    model_dict[USER_TYPE] = load_profile_anomaly_model_for_actor_type(spark=spark, data_source=data_source,
                                                                      actor_type=USER_TYPE)
    model_dict[ENTITY_TYPE] = load_profile_anomaly_model_for_actor_type(spark=spark, data_source=data_source,
                                                                        actor_type=ENTITY_TYPE)
    return model_dict


def get_profile_anomaly_score(model_dict, input_df, actor_type, time_window):
    result_df = model_dict[actor_type][time_window].score(input_df)
    score = result_df["PAS"][0]
    return score

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = vars(ap.parse_args())
    params = json.loads(args["params"])
    data_source = params["data_source"]

    log_path = get_log_path(data_source, "profile_anomaly_batch_score")
    logger = configure_logger(logger_name="profile_anomaly_batch_score", log_path=log_path, log_level=LOG_LEVEL)
    logger.info(data_source)

    app_name = data_source + "_profile_anomaly_batch_score"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel(PYSPARK_LOGLEVEL)

    model_dict = load_profile_anomaly_model(spark=spark, data_source=data_source)
    logger.info(model_dict)

    json_req = {'patient_id': "fhaksd", 'maximum_claim_amount': "40370", 'minimum_claim_amount': "2850",
                'total_claim_amount': "121210", 'total_claim_count': "5"}
    df = pd.DataFrame(
        data=[[json_req["patient_id"], json_req["maximum_claim_amount"], json_req["minimum_claim_amount"],
               json_req["total_claim_amount"], json_req["total_claim_count"]]],
        columns=['patient_id', 'maximum_claim_amount', 'minimum_claim_amount',
                 'total_claim_amount', 'total_claim_count'])
    score = get_profile_anomaly_score(model_dict=model_dict, input_df=df, actor_type=USER_TYPE, time_window="two_minute")
    logger.info(score)


if __name__ == "__main__":
    main()
