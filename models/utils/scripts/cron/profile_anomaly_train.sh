#!/usr/bin/env bash

#export PYTHONPATH=/home/bivav/spark-2.1.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip
#export PYSPARK_PYTHON=/home/bivav/.virtualenvs/dataAnalytics_models_py3/bin/python
#export PYSPARK_DRIVER_PYTHON=/home/bivav/.virtualenvs/dataAnalytics_models_py3/bin/python
#export PYTHONPATH="/home/bivav/dataAndAnalytics/models":$PYTHONPATH
#/home/bivav/spark-2.1.1-bin-hadoop2.7/bin/spark-submit /home/bivav/dataAndAnalytics/models/model_platform/src/operationalization/anomaly/profile_anomaly/train.py --params '{"data_source":"default"}'

# Below is for the remote server.
# Uncomment when deploying it to server and comment above.

export PYTHONPATH=/Users/tuhinsharma/spark-2.1.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip
export PYSPARK_PYTHON=/Users/tuhinsharma/.virtualenvs/hospital-claim-fraud-detection-v3/bin/python
export PYSPARK_DRIVER_PYTHON=/Users/tuhinsharma/.virtualenvs/hospital-claim-fraud-detection-v3/bin/python
export PYTHONPATH="/Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/models":$PYTHONPATH
/Users/tuhinsharma/spark-2.1.1-bin-hadoop2.7/bin/spark-submit /Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/models/model_platform/src/operationalization/anomaly/profile_anomaly/train.py --params '{"data_source":"default"}'
