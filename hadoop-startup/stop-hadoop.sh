#!/bin/bash

# ========================
# Author: Kartik
# Date: 17-11-2024
# Description: This script stops all the Hadoop daemons (HDFS and YARN).
# It stops HDFS, YARN, and any related services.
# ========================

# Define color variables for convenience
RED="\033[0;31m"
RESET="\033[0m"

# Stop YARN services
echo -e "${RED}Stopping YARN...${RESET}"
./run-yarn.sh -s stop

# Stop Hadoop services (HDFS)
echo -e "${RED}Stopping HDFS...${RESET}"
./run-hdfs.sh -s stop

# Check the status of the running processes
echo -e "${RED}Checking running processes...${RESET}"
jps

