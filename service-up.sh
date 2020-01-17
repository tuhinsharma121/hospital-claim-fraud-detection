#!/usr/bin/env bash


echo "=================================================================================="
echo "Spark service bringing up - started"
echo "=================================================================================="

cd ~
nohup spark-2.1.1-bin-hadoop2.7/sbin/start-all.sh >> ~/deploy-logs/nohup_spark.out&
sleep 15
ps aux | grep spark

echo "=================================================================================="
echo "Spark service bringing up - ended"
echo "=================================================================================="


echo "=================================================================================="
echo "Zookeeper service bringing up - started"
echo "=================================================================================="

cd ~
nohup zookeeper-3.4.13/bin/zkServer.sh start zookeeper-3.4.13/conf/zoo.cfg >> ~/deploy-logs/nohup_zookeeper.out&
sleep 15
ps aux | grep zookeeper

echo "=================================================================================="
echo "Zookeeper service bringing up - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Kafka service bringing up - started"
echo "=================================================================================="

cd ~
nohup kafka_2.12-2.0.0/bin/kafka-server-start.sh kafka_2.12-2.0.0/config/server.properties >> ~/deploy-logs/nohup_kafka.out&
sleep 15
ps aux | grep kafka

echo "=================================================================================="
echo "Kafka service bringing up - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Druid service bringing up - started"
echo "=================================================================================="

cd ~
nohup apache-druid-0.14.1-incubating/bin/supervise -c apache-druid-0.14.1-incubating/quickstart/tutorial/conf/tutorial-cluster.conf >> ~/deploy-logs/nohup_druid.out&
echo $! > ~/druid-dir/run.pid
sleep 15
ps aux | grep druid

echo "=================================================================================="
echo "Druid service bringing up - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Flink service bringing up - started"
echo "=================================================================================="

cd ~
nohup flink-1.8.0/bin/start-cluster.sh >> ~/deploy-logs/nohup_flink.out&
sleep 15
ps aux | grep flink

echo "=================================================================================="
echo "Flink service bringing up - ended"
echo "=================================================================================="
