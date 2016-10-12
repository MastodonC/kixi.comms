#!/usr/bin/env bash
# https://hub.docker.com/r/ches/kafka/
docker rm zookeeper
docker rm kafka
docker run -d --name zookeeper -p 2181:2181 jplock/zookeeper:3.4.6
docker run -d --name kafka --link zookeeper:zookeeper -p 9092:9092 ches/kafka:0.10.0.1
sleep 8s
docker run --net=host --rm ches/kafka kafka-topics.sh --create --topic command --replication-factor 1 --partitions 1 --zookeeper localhost
docker run --net=host --rm ches/kafka kafka-topics.sh --create --topic event --replication-factor 1 --partitions 1 --zookeeper localhost
