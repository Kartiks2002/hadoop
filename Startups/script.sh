#!/bin/bash

# Author : Kartik
# Created on : 20-11-2024
# Last Modified : 20-11-2024

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

HDFS_DIR="/user/cloudera"

# Help function
show_help() {
    echo -e "${GREEN}Usage: ./automation_script.sh${NC}"
    echo -e "This script automates the process of uploading a CSV file to HDFS, creating a Hive table, and running Hive queries."
    echo -e "Options:"
    echo -e "  -h, --help    Show this help message"
    echo -e "  Make sure the file 'Listofstartups.csv' is in the same directory as the script."
}

# Check if the help flag is passed
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

# Check if the CSV file exists locally
if [ ! -f "Listofstartups.csv" ]; then
    echo -e "${RED}Error: CSV file '$LOCAL_CSV_FILE' not found in the current directory.${NC}"
    exit 1
fi

# check if the csv file already exists on the hdfs
hdfs dfs -test -f $HDFS_DIR/Listofstartups.csv
if [ $? -eq 0 ]; then
	echo -e "${GREEN}File Listofstartups.csv already exists in HDFS. Removing it...${NC}"
	hdfs dfs -rm $HDFS_DIR/Listofstartups.csv
	if [ $? -eq 0 ]; then
		echo -e "${GREEN}File successfully removed.${NC}"
	else
		echo -e "${RED}Error: Failed to remove the file from HDFS.${NC}"
		exit 1
	fi
fi

# put the csv file on the hdfs from local
echo -e "${GREEN}Uploading Listofstartups.csv to HDFS...${NC}"
hdfs dfs -put Listofstartups.csv $HDFS_DIR/
if [ $? -eq 0 ]; then
	echo -e "${GREEN}File uploaded successfully to HDFS. ${NC}"
else
	echo -e "${RED}Error: Failed to upload the file to HDFS.${NC}"
	exit 1
fi
# execute Hive query from the hql script
echo -e "${GREEN}Executing Hive queries from queries.hql...${NC}"
hive -f queries.hql
if [ $? -eq 0 ]; then
    echo "${GREEN}Hive queries executed successfully.${NC}"
else
    echo "${RED}Error: Hive query execution failed.${NC}"
    exit 1
fi

