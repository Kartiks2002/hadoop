#!/bin/bash

# ========================
# Author: Kartik
# Date: 17-11-2024
# Description: This script automates the process of starting the Hadoop ecosystem (HDFS and YARN).
# It formats the NameNode (only on initial setup), starts HDFS and YARN services, creates a user directory in HDFS,
# and checks the status of the running processes.
# ========================

# Define color variables for convenience
BLUE="\033[0;34m"
RESET="\033[0m"

# Format the NameNode (only run once during initial setup)
echo -e "${BLUE}Formatting the NameNode...${RESET}"
hdfs namenode -format

# Start Hadoop services
echo -e "${BLUE}Starting HDFS...${RESET}"
./run-hdfs.sh -s start

# Start YARN services
echo -e "${BLUE}Starting YARN...${RESET}"
./run-yarn.sh -s start

# Create the user directory in HDFS
echo -e "${BLUE}Creating user directory in HDFS...${RESET}"
hdfs dfs -mkdir -p /user/talentum

# Check the status of the running processes
echo -e "${BLUE}Checking running processes...${RESET}"
jps

