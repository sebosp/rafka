#!/bin/bash

# Script to generate kafka 2.2.1 real data that then can be used to test rafka compatibility.

rm -rf ./2_2_1_logs/
mkdir -p 2_2_1_logs/

docker kill zkp
docker rm -v zkp
docker run -d --name zkp -p 2181:2181 zookeeper:3.6.2

docker kill kfk1
docker rm -v kfk1
docker run -d -v $PWD/2_2_1_logs:/bitnami/ --name kfk1 -e KAFKA_CFG_ZOOKEEPER_CONNECT=zkp:2181 -e ALLOW_PLAINTEXT_LISTENER=yes --link zkp docker.io/bitnami/kafka:2.2.1
sleep 5
docker exec kfk1 chmod -R a+rw /bitnami/kafka/data
sleep 5
docker kill kfk1
docker rm -v kfk1
docker kill zkp
docker rm -v zkp

sudo chmod -R a+rw 2_2_1_logs/
