#!/usr/bin/env bash

echo "=================================================================================="
echo "OS dependencies installation  - started"
echo "=================================================================================="

cd ~
sudo apt-get -y update --fix-missing
sudo apt-get -y install build-essential
sudo apt-get -y install zlib1g-dev
sudo apt-get install -y libssl-dev libncurses5-dev libsqlite3-dev libreadline-dev libtk8.5 libgdm-dev libdb4o-cil-dev libpcap-dev
sudo apt-get install -y python-minimal
sudo apt-get -y install libblas-dev libatlas-base-dev
sudo apt-get -y install libsasl2-dev
sudo apt-get -y install docker.io

echo "=================================================================================="
echo "OS dependencies installation  - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Java openjdk-8-jdk installation - started"
echo "=================================================================================="

cd ~
sudo apt-get -y install openjdk-8-jdk

echo "=================================================================================="
echo "Java openjdk-8-jdk installation - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "sbt installation - started"
echo "=================================================================================="

cd ~
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get -y install sbt

echo "=================================================================================="
echo "sbt installation - started"
echo "=================================================================================="

echo "=================================================================================="
echo "Python 3.6.5 installation - started"
echo "=================================================================================="

cd ~
wget https://www.python.org/ftp/python/3.6.5/Python-3.6.5.tgz
tar -xzf Python-3.6.5.tgz
cd Python-3.6.5
sudo ./configure
sudo make
sudo make install

echo "=================================================================================="
echo "Python 3.6.5 installation - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Zookeeper set up - started"
echo "=================================================================================="

cd ~
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.13/zookeeper-3.4.13.tar.gz
tar -xf zookeeper-3.4.13.tar.gz
cp zookeeper-3.4.13/conf/zoo_sample.cfg zookeeper-3.4.13/conf/zoo.cfg
rm -rf zookeeper-data
mkdir zookeeper-data
sudo chmod 777 zookeeper-data
sed -ie "s/dataDir=\/tmp\/zookeeper/dataDir=zookeeper-data/g" zookeeper-3.4.13/conf/zoo.cfg
rm -rf zookeeper-3.4.13/conf/zoo.cfge

echo "=================================================================================="
echo "Zookeeper set up - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Kafka set up - started"
echo "=================================================================================="

cd ~
wget https://archive.apache.org/dist/kafka/2.0.0/kafka_2.12-2.0.0.tgz
tar -xzf kafka_2.12-2.0.0.tgz
rm -rf kafka-logs
mkdir kafka-logs
sudo chmod 777 kafka-logs
sed -ie "s/log.dirs=\/tmp\/kafka-logs/log.dirs=kafka-logs/g" kafka_2.12-2.0.0/config/server.properties
rm -rf kafka_2.12-2.0.0/config/server.propertiese

echo "=================================================================================="
echo "Kafka set up - ended"
echo "=================================================================================="


echo "=================================================================================="
echo "Druid set up - started"
echo "=================================================================================="

cd ~
rm -rf druid-dir
mkdir druid-dir
sudo chmod 777 druid-dir
wget https://archive.apache.org/dist/incubator/druid/0.14.1-incubating/apache-druid-0.14.1-incubating-bin.tar.gz
tar -xf apache-druid-0.14.1-incubating-bin.tar.gz
cd apache-druid-0.14.1-incubating
wget http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.3.tgz
tar -xzf tranquility-distribution-0.8.3.tgz
mv tranquility-distribution-0.8.3 tranquility
sed -ie '14,14 s/^##*//' quickstart/tutorial/conf/tutorial-cluster.conf
sed -ie '5,5 s/^/#/g' quickstart/tutorial/conf/tutorial-cluster.conf
sed -ie '2,2 s/^/#/g' quickstart/tutorial/conf/tutorial-cluster.conf
rm -rf quickstart/tutorial/conf/tutorial-cluster.confe

sed -ie "s/zk.host.ip/\"localhost:2181\"/g" conf/druid/_common/common.runtime.properties
rm -rf conf/druid/_common/common.runtime.propertiese
sed -ie "s/zk.host.ip/\"localhost:2181\"/g" quickstart/tutorial/conf/druid/_common/common.runtime.properties
rm -rf quickstart/tutorial/conf/druid/_common/common.runtime.propertiese


sed -ie "s/druid.plaintextPort=8082/druid.plaintextPort=28082/g" conf/druid/broker/runtime.properties
rm -rf conf/druid/broker/runtime.propertiese
sed -ie "s/druid.plaintextPort=8082/druid.plaintextPort=28082/g" quickstart/tutorial/conf/druid/broker/runtime.properties
rm -rf quickstart/tutorial/conf/druid/broker/runtime.propertiese

sed -ie "s/druid.plaintextPort=8081/druid.plaintextPort=28081/g" conf/druid/coordinator/runtime.properties
rm -rf conf/druid/coordinator/runtime.propertiese
sed -ie "s/druid.plaintextPort=8081/druid.plaintextPort=28081/g" quickstart/tutorial/conf/druid/coordinator/runtime.properties
rm -rf quickstart/tutorial/conf/druid/coordinator/runtime.propertiese

sed -ie "s/druid.plaintextPort=8083/druid.plaintextPort=28083/g" conf/druid/historical/runtime.properties
rm -rf conf/druid/historical/runtime.propertiese
sed -ie "s/druid.plaintextPort=8083/druid.plaintextPort=28083/g" quickstart/tutorial/conf/druid/historical/runtime.properties
rm -rf quickstart/tutorial/conf/druid/historical/runtime.propertiese

sed -ie "s/druid.plaintextPort=8091/druid.plaintextPort=28091/g" conf/druid/middleManager/runtime.properties
rm -rf conf/druid/middleManager/runtime.propertiese
sed -ie "s/druid.plaintextPort=8091/druid.plaintextPort=28091/g" quickstart/tutorial/conf/druid/middleManager/runtime.properties
rm -rf quickstart/tutorial/conf/druid/middleManager/runtime.propertiese

sed -ie "s/druid.plaintextPort=8090/druid.plaintextPort=28090/g" conf/druid/overlord/runtime.properties
rm -rf conf/druid/overlord/runtime.propertiese
sed -ie "s/druid.plaintextPort=8090/druid.plaintextPort=28090/g" quickstart/tutorial/conf/druid/overlord/runtime.properties
rm -rf quickstart/tutorial/conf/druid/overlord/runtime.propertiese

sed -ie "s/druid.plaintextPort=8888/druid.plaintextPort=28888/g" conf/druid/router/runtime.properties
rm -rf conf/druid/router/runtime.propertiese
sed -ie "s/druid.plaintextPort=8888/druid.plaintextPort=28888/g" quickstart/tutorial/conf/druid/router/runtime.properties
rm -rf quickstart/tutorial/conf/druid/router/runtime.propertiese

echo "=================================================================================="
echo "Druid set up - ended"
echo "=================================================================================="

echo "=================================================================================="
echo "Flink set up - started"
echo "=================================================================================="

cd ~
rm -rf deploy-logs
mkdir deploy-logs
sudo chmod 777 deploy-logs
wget https://archive.apache.org/dist/flink/flink-1.8.0/flink-1.8.0-bin-scala_2.11.tgz
tar -xzf flink-1.8.0-bin-scala_2.11.tgz
sed -ie "s/#rest.port: 8081/rest.port: 18081/g" flink-1.8.0/conf/flink-conf.yaml
rm -rf flink-1.8.0/conf/flink-conf.yamle

echo "=================================================================================="
echo "Flink set up - ended"
echo "=================================================================================="

echo "Spark set up - started"
cd ~
wget https://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz
tar -xzf spark-2.1.1-bin-hadoop2.7.tgz
echo "Spark set up - ended"

echo "=================================================================================="
echo "=================================================================================="