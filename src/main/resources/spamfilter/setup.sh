#!/bin/bash

TRAINING_TOPIC="training"
ANALYSIS_TOPIC="analysis"

# create topics on kafka
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TRAINING_TOPIC
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $ANALYSIS_TOPIC