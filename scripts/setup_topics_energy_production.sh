#!/bin/bash

echo "Creating topic: energy.production"

docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic energy.production --partitions 4 --replication-factor 3 --config min.insync.replicas=2 --if-not-exists

echo "Listing topics"

docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list

echo "Describing topic"

docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic energy.production

echo "Done!"
