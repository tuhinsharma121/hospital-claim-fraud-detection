#!/usr/bin/env bash

cd /Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/models/model_platform/deployment/
#$DEPLOY_MODEL_DIR
export PYTHONPATH=/Users/tuhinsharma/spark-2.1.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip
export PYSPARK_PYTHON=/Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/models/dataAnalytics_models_py3/bin/python3
export PYSPARK_DRIVER_PYTHON=/Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/models/dataAnalytics_models_py3/bin/python3
export PYTHONPATH="/Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/models":$PYTHONPATH
export PYTHONPATH="/Users/tuhinsharma/spark-2.1.1-bin-hadoop2.7/python":$PYTHONPATH

## If you choose to run with spark-submit, uncomment spark-submit and comment gunicorn
gunicorn --pythonpath / -b 0.0.0.0:34008 -k gevent -t 1000 app:app -w 1
#/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit /root/dataAndAnalytics/models/model_platform/deployment/app.py
