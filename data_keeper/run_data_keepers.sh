#!/bin/bash
# filepath: /home/daniel-nabil/Desktop/University/Wireless/DistributedFileSystem/run_data_keepers.sh

# File to store process IDs
PID_FILE=".data_keeper_pids"
> $PID_FILE  # Clear any existing content

# Function to kill all spawned processes
cleanup() {
  echo "Shutting down all data keeper nodes..."
  if [ -f $PID_FILE ]; then
    while read pid; do
      if ps -p $pid > /dev/null; then
        echo "Killing process $pid"
        kill $pid
      fi
    done < $PID_FILE
    rm $PID_FILE
  fi
  echo "All data keeper nodes terminated."
  exit 0
}

# Register the cleanup function to be called on Ctrl+C
trap cleanup INT TERM

# Start Data Keeper 1
go run data_keeper/main.go -name="DataKeeper-1" -ports="50051 50052" -master_ip="127.0.0.1" -master_port="50050" &
echo $! >> $PID_FILE

# Start Data Keeper 2
go run data_keeper/main.go -name="DataKeeper-2" -ports="50053 50054" -master_ip="127.0.0.1" -master_port="50050" &
echo $! >> $PID_FILE  

# Start Data Keeper 3
go run data_keeper/main.go -name="DataKeeper-3" -ports="50055 50056" -master_ip="127.0.0.1" -master_port="50050" &
echo $! >> $PID_FILE

echo "Started 3 data keeper nodes"
echo "Press Ctrl+C to terminate all data keeper nodes"

# Keep the script running until user presses Ctrl+C
wait
# to run client 
#go run client/main.go -port 50059 -master_ip 192.168.38.222