# HospitalClaim Fraud Analytics

## Clone the repo
```bash
https://github.com/tuhinsharma121/hospital-claim-fraud-detection.git
```

## Switch to master branch

```bash
cd hospital-claim-fraud-detection
git checkout master
cd ..
```

## To configure the machine with Zookeeper, Kafka, Druid, Flink, Spark

### For ubuntu 16.04
```bash
hospital-claim-fraud-detection/ubuntu-1604-install-dependency.sh
```

### For osx
```bash
hospital-claim-fraud-detection/osx-install-dependency.sh
```


## To bring the services up for Zookeeper, Kafka, Druid, Flink, Spark
For mac make sure remote login is enabled.
```bash
hospital-claim-fraud-detection/service-up.sh
```

### To access the web ui 

1. FLINK - http://localhost:18081
2. DRUID - http://localhost:28888
3. SPARK - http://localhost:8080

## To bring the services down for Zookeeper, Kafka, Druid, Flink, Spark

```bash
hospital-claim-fraud-detection/service-down.sh
```

## To reset the services for Zookeeper, Kafka, Druid, Flink, Spark

```bash
hospital-claim-fraud-detection/service-reset.sh
```


# Start the Flink job

## Build the jar

```bash
cd hospital-claim-fraud-detection/profiler
sbt clean assembly
cd ~
```

## To run the profiler (note down the pid)
```bash
cd flink-1.8.0
nohup ./bin/flink run -c com.hospitalclaim.fraud.profilers.Profiler /Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/profiler/target/scala-2.11/hospital-claim-fraud-profiler-assembly-0.1.0-SNAPSHOT.jar --config /Users/tuhinsharma/Documents/Git/hospital-claim-fraud-detection/profiler/src/main/resources/hospital-claim-fraud-profiler.properties > /Users/tuhinsharma/nohup_flink_profiler.out&
```

## To stop the profiler
```bash
kill -9 $pid
```

# HospitalClaim Fraud Models

## Configure a virtual environment

## Install python dependencies (Every time `requirements.txt` is changed)

```bash
pip3 install -r hospital-claim-fraud-detection/models/requirements.txt
```

## To set up kafka-druid for data ingestion

```bash
nohup python3.6 hospital-claim-fraud-detection/models/utils/scripts/druid_setup.py > ~/nohup_druid_setup.out&
```

## To start the dummy data generator (note down the pid to stop it later)

```bash
nohup python3.6 hospital-claim-fraud-detection/models/utils/scripts/kafka_claimdata_producer.py > ~/nohup_kafka_datagen.out&
```

## To stop the dummy data generator
```bash
kill -9 $pid
```

## To run the model training 
```bash
bash hospital-claim-fraud-detection/models/utils/scripts/cron/profile_anomaly_train.sh
```

## To run the app module
```bash
bash hospital-claim-fraud-detection/models/utils/scripts/cron/profile_anomaly_deploy.sh
```


