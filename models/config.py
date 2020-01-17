import json

# path for tenant related data
TENANT_NAME = "claim"
BASE_PATH = "~/" + TENANT_NAME

LOG_FOLDER = "~/deploy-logs/"
LOG_LEVEL = "INFO"
PYSPARK_LOGLEVEL = "ERROR"

# repositary path
ANOMALY_MODEL_REPOSITORY = BASE_PATH + "/models_data/model"
ANOMALY_DATA_REPOSITORY = BASE_PATH + "/models_data/data"

# training data path
USER_PROFILE_DATA_PATH = ANOMALY_DATA_REPOSITORY + "/{data_source}/{entity_type}/{anomaly_type}/{time_window}.json"

# model paths
PROFILE_ANOMALY_MODEL_PATH = ANOMALY_MODEL_REPOSITORY + "/{data_source}/{anomaly_type}/{entity_type}/{time_window}/{model_name}"
EVENT_ANOMALY_MODEL_PATH = ANOMALY_MODEL_REPOSITORY + "/{data_source}/{anomaly_type}/{model_name}"

# time window

TIME_WINDOW_TIMESTAMP_DICT = {"two_minute": 2 * 60,
                              "three_minute": 3 * 60}

TIME_WINDOW_LIST = TIME_WINDOW_TIMESTAMP_DICT.keys()

# entity types
USER_TYPE = "patient"
ENTITY_TYPE = "hospital"

# anomaly types
PROFILE_ANOMALY_TYPE = "profile"
EVENT_ANOMALY_TYPE = "event"

# model types
PYSPARK_KMEANS_MODEL = "pyspark_kmeans"
SKLEARN_ISOLATION_FOREST_MODEL = "sklearn_isolationforest"
SKLEARN_ONECLASS_SVM_MODEL = "sklearn_oneclasssvm"

TELEGRAM_URL = "https://api.telegram.org/bot721678688:AAEMIk-IOrJvMfm80-GuLxQa0MpiK1cbIHQ/sendMessage"
TELEGRAM_CHANNEL_ID = "@attmodelslogger"

SLACK_CHANNEL = "#att-models-log"
SLACK_BOT_TOKEN = "xoxb-501492022263-574304627685-xkt9wroHCGig8NQhZyJYinkt"

DEPLOYMENT_TYPE = "dev"