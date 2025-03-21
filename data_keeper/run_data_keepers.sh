#!/bin/bash

# Start Data Keeper 1
go run data_keeper/main.go -name="DataKeeper-1" -ports="50051 50052" -master_ip="127.0.0.1" -master_port="50050" -storage="./storage1" &

# Start Data Keeper 2
go run data_keeper/main.go -name="DataKeeper-2" -ports="50053 50054" -master_ip="127.0.0.1" -master_port="50050" -storage="./storage2" &

# Start Data Keeper 3
go run data_keeper/main.go -name="DataKeeper-3" -ports="50055 50056" -master_ip="127.0.0.1" -master_port="50050" -storage="./storage3" &

echo "Started 3 data keeper nodes"