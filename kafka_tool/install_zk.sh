#!/bin/bash

broker_num=${1}
broker_id=${2}
brokers=()
args=(${@})
echo ${args[$a]}
for (( i=0 ; i<$broker_num ; i++ ));
do
	idx=`expr 2 + $i`
	echo broker : ${args[$idx]}
	brokers+=(${args[$idx]})
done

echo cluster_size : ${broker_num}
echo broker id    : ${broker_id}
echo ------------------------------------------------------

# jdk install
echo Installing JDK8...
sudo apt-get update
sudo apt-get install openjdk-8-jdk -y

# install zookeeper
echo Installing zookeeper...
sudo wget https://downloads.apache.org/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz
sudo tar zxf apache-zookeeper-3.6.2-bin.tar.gz
sudo ln -s apache-zookeeper-3.6.2-bin zookeeper

# set cluster broker id
echo Setting broker id
sudo mkdir -p ${PWD}/data
sudo echo ${broker_id} > ${PWD}/data/myid

# set host info
echo Setting hosts info

echo >> /etc/hosts
echo "# zookeeper server info" >> /etc/hosts
for ((i=0 ; i<${broker_num} ; i++));
do
	idx=`expr $i + 1`
	if [ $idx -eq $broker_id ];then
		echo 0.0.0.0 zoo$idx >> /etc/hosts
	else
		echo ${brokers[$i]} zoo$idx >> /etc/hosts
	fi
done

# set zookeeper conf
echo Setting zookeeper config
echo tickTime=2000 > ${PWD}/zookeeper/conf/zoo.cfg
echo dataDir=${PWD}/data >> ${PWD}/zookeeper/conf/zoo.cfg
echo clientPort=2181 >> ${PWD}/zookeeper/conf/zoo.cfg
echo initLimit=20 >> ${PWD}/zookeeper/conf/zoo.cfg
echo syncLimit=5 >> ${PWD}/zookeeper/conf/zoo.cfg

for ((i=0 ; i<${broker_num} ; i++));
do
	idx=`expr $i + 1`
	echo server.$idx=zoo$idx:2888:3888 >> ${PWD}/zookeeper/conf/zoo.cfg
done

