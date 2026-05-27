#!/bin/bash
 
echo "Creating topic: energy.production"
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic energy.production --partitions 4 --replication-factor 3 --config min.insync.replicas=2 --if-not-exists
 
echo "Creating topic: energy.consumption"
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic energy.consumption --partitions 4 --replication-factor 3 --config min.insync.replicas=2 --if-not-exists
 
echo "Creating topic: energy.alerts"
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic energy.alerts --partitions 4 --replication-factor 3 --config min.insync.replicas=2 --if-not-exists
 
echo "Listing all topics:"
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list
 
echo "Topic details for energy.production:"
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic energy.production
 
echo "Topic details for energy.consumption:"
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic energy.consumption
 
echo "Topic details for energy.alerts:"
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic energy.alerts
 
echo "Topics created successfully!"