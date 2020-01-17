#!/usr/bin/env bash

echo "=================================================================================="
echo "Spark service bringing down - started"
echo "=================================================================================="

cd ~
nohup spark-2.1.1-bin-hadoop2.7/sbin/stop-all.sh >> ~/deploy-logs/nohup_spark.out&
sleep 15
ps aux | grep spark

echo "=================================================================================="
echo "Spark service bringing down - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Flink service bringing down - started"
echo "=================================================================================="

cd ~
nohup flink-1.8.0/bin/stop-cluster.sh >> ~/deploy-logs/nohup_flink.out&
sleep 15
ps aux | grep flink

echo "=================================================================================="
echo "Flink service bringing down - ended"
echo "=================================================================================="


echo "=================================================================================="
echo "Druid service bringing down - started"
echo "=================================================================================="

cd ~
line=$(head -n 1 ~/druid-dir/run.pid)
kill $line
sleep 30
ps aux | grep druid

echo "=================================================================================="
echo "Druid service bringing down - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Kafka service bringing down - started"
echo "=================================================================================="

cd ~
nohup kafka_2.12-2.0.0/bin/kafka-server-stop.sh >> ~/deploy-logs/nohup_kafka.out&
sleep 30
ps aux | grep kafka

echo "=================================================================================="
echo "Kafka service bringing down - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Zookeeper service bringing down - started"
echo "=================================================================================="

cd ~
nohup zookeeper-3.4.13/bin/zkServer.sh stop zookeeper-3.4.13/conf/zoo.cfg >> ~/deploy-logs/nohup_zookeeper.out&
sleep 15
ps aux | grep zookeeper

echo "=================================================================================="
echo "Zookeeper service bringing down - ended"
echo "=================================================================================="

