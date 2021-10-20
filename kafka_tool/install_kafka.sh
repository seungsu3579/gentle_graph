#!/bin/bash

broker_num=${1}
broker_id=${2}
znode_name=${3}
zk_brokers=()
args=(${@})
echo ${args[$a]}
for (( i=0 ; i<$broker_num ; i++ ));
do
	idx=`expr 3 + $i`
	echo zk broker `expr $1 + 1` : ${args[$idx]}
	zk_brokers+=(${args[$idx]})
done

echo kafka znode     : ${znode_name}
echo kafka broker id : ${broker_id}
echo ------------------------------------------------------

# jdk install
echo @ Installing JDK8...
sudo apt-get update
sudo apt-get install openjdk-8-jdk -y

# install kafak
echo @ Installing kafka...
sudo wget http://apache.mirror.cdnetworks.com/kafka/2.7.0/kafka_2.13-2.7.0.tgz
sudo tar zxf kafka_2.13-2.7.0.tgz
sudo ln -s kafka_2.13-2.7.0 kafka

# set kafka data directory
echo @ Setting kafka data directory
sudo mkdir -p ${PWD}/kafka_data1

# set host info
# echo @ Setting zookeeper hosts info

# echo >> /etc/hosts
# echo "# zookeeper server info" >> /etc/hosts
# for ((i=0 ; i<${broker_num} ; i++));
# do
# 	idx=`expr $i + 1`
# 	if [ $idx -eq $broker_id ];then
# 		echo 0.0.0.0 zoo$idx >> /etc/hosts
# 	else
# 		echo ${zk_brokers[$i]} zoo$idx >> /etc/hosts
# 	fi
# done

# set kafka conf
echo @ Setting kafka config

config_path=${PWD}/kafka/config/server.properties
echo >> $config_path
echo "# kafka info" >> $config_path
echo broker.id=${broker_id} >> $config_path
echo log.dirs=${PWD}/kafka_data1 >> $config_path

zk_servers="zookeeper.connect="

for ((i=0 ; i<${broker_num} ; i++));
do
	idx=`expr $i + 1`
	zk_servers=$zk_servers"zoo${idx}:2181,"
done
zk_servers=${zk_servers:0:-1}
echo $zk_servers"/$znode_name" >> $config_path
