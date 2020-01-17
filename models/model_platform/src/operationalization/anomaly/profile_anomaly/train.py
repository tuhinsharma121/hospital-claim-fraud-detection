import argparse

from pyspark.sql import SparkSession

from config import *
from model_platform.src.model.anomaly.profile_model.isolation_forest_profile_model import \
    IsolationForestProfileModel
from utils.data_store.druid_data_store import DruidDataStore
import datetime

from utils.logging.notify_error import send_error_notification
from utils.logging.pylogger import get_log_path, configure_logger


def train_and_save_sklean_isolationforest_profile_anomaly_model_for_actor(spark, data_store, data_source,
                                                                          categorical_colnames,
                                                                          numerical_colnames, time_window, sdf,
                                                                          data_path,
                                                                          actor_type, logger):
    logger.info("###################################################################################")
    logger.info(
        "sklean_isolationforest profile anomaly model training for {actor_type} for {data_source} for time window {window} started".format(
            actor_type=actor_type,
            data_source=data_source, window=time_window))

    model = IsolationForestProfileModel.train(data_store=data_store, categorical_colnames=categorical_colnames,
                                              numerical_colnames=numerical_colnames,
                                              data_path=data_path, data_source=data_source,
                                              actor_type=actor_type,
                                              anomaly_type=PROFILE_ANOMALY_TYPE,
                                              sdf=sdf,
                                              time_window=time_window)

    model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                   entity_type=actor_type, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                   time_window=time_window,
                                                   model_name=SKLEARN_ISOLATION_FOREST_MODEL)
    model.save(spark=spark, path=model_path)
    logger.info(
        "sklean_isolationforest profile anomaly model training for {actor_type} for {data_source} for time window {window} ended".format(
            actor_type=actor_type,
            data_source=data_source, window=time_window))
    logger.info("###################################################################################")


def train_and_save_profile_anomaly_model_for_actor(spark, data_store, data_source, categorical_colnames,
                                                   numerical_colnames, actor_type, logger):
    logger.info("###################################################################################")
    logger.info(
        "profile anomaly model training for {actor_type} for {data_source} started".format(actor_type=actor_type,
                                                                                           data_source=data_source))
    for time_window in TIME_WINDOW_LIST:

        data_path = USER_PROFILE_DATA_PATH.format(data_source=data_source, entity_type=actor_type,
                                                  anomaly_type=PROFILE_ANOMALY_TYPE,
                                                  time_window=time_window)

        sdf = data_store.read_spark_df_from_data_store(spark=spark,
                                                       data_source=data_source,
                                                       actor_type=actor_type,
                                                       table=actor_type + "_profile_" + time_window
                                                       )

        sdf.show(3)

        train_and_save_sklean_isolationforest_profile_anomaly_model_for_actor(spark=spark, data_store=data_store,
                                                                              data_source=data_source,
                                                                              categorical_colnames=categorical_colnames,
                                                                              numerical_colnames=numerical_colnames,
                                                                              data_path=data_path,
                                                                              time_window=time_window, sdf=sdf,
                                                                              actor_type=actor_type,
                                                                              logger=logger)

    logger.info("profile anomaly model training for {actor_type} for {data_source} ended".format(actor_type=actor_type,
                                                                                                 data_source=data_source))
    logger.info("###################################################################################")


def train_and_save_profile_anomaly_model(spark, data_store, data_source,
                                         logger):
    logger.info("###################################################################################")
    logger.info("profile anomaly model training for {data_source} started".format(data_source=data_source))
    train_and_save_profile_anomaly_model_for_actor(spark=spark, data_store=data_store, data_source=data_source,
                                                   categorical_colnames=[],
                                                   numerical_colnames=['maximum_claim_amount', 'maximum_patient_age',
                                                                       'minimum_claim_amount',
                                                                       'minimum_patient_age', 'total_claim_amount',
                                                                       'total_claim_count'], actor_type=ENTITY_TYPE,
                                                   logger=logger)
    train_and_save_profile_anomaly_model_for_actor(spark=spark, data_store=data_store, data_source=data_source,
                                                   categorical_colnames=[],
                                                   numerical_colnames=['maximum_claim_amount',
                                                                       'minimum_claim_amount',
                                                                       'total_claim_amount',
                                                                       'total_claim_count'], actor_type=USER_TYPE,
                                                   logger=logger)
    logger.info("profile anomaly model training for {data_source} ended".format(data_source=data_source))
    logger.info("###################################################################################")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = vars(ap.parse_args())
    params = json.loads(args["params"])
    data_source = params["data_source"]

    log_path = get_log_path(data_source, "profile_anomaly_train")
    logger = configure_logger(logger_name="profile_anomaly_train", log_path=log_path, log_level=LOG_LEVEL)

    logger.info(data_source)

    app_name = data_source + "_profile_anomaly_train"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel(PYSPARK_LOGLEVEL)

    try:
        # data_store = HDFSDataStore(spark=spark)
        # data_store = HBaseDataStore(spark=spark)
        data_store = DruidDataStore(spark=spark)
        train_and_save_profile_anomaly_model(spark=spark, data_store=data_store, data_source=data_source, logger=logger)
    except Exception as e:
        logger.exception(e)
        send_error_notification()


if __name__ == "__main__":
    main()
