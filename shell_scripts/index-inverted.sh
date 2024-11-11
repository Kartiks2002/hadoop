#!/bin/bash

# Author: Kartik
# Created on: 11th Nov 2024 (18:48)
# Last Modified : 11th Nov 2024

# Purpose : Export the IndexInverter Project to a jar file.


GREEN='\033[0;32m'
NC='\033[0m' 

# Define paths for the source code, output directories, and dependencies
SRC_DIR="/home/cloudera/workspace/IndexInverter/src"
OUTPUT_DIR="/home/cloudera/workspace/IndexInverter/build"
JAR_NAME="IndexInverterJob.jar"
MAIN_CLASS_NAME="inverted.IndexInverterJob"
NO_REDUCERS=${1:-1}

#creating the o/p directory 
mkdir -p $OUTPUT_DIR

#compiling java soucre files
echo -e "${GREEN}Compiling Java files...${NC}"
javac -classpath $(hadoop classpath) -d $OUTPUT_DIR $SRC_DIR/inverted/*.java

#creating the jar file
echo -e "${GREEN}Creating JAR file...${NC}"
jar -cvf $OUTPUT_DIR/$JAR_NAME -C $OUTPUT_DIR/ .

#run the jar file using yarn
echo -e "${GREEN}}Running the application...${NC}"

hdfs dfs -test -d IndexInverter/out
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Removing existing output directory in HDFS...${NC}"
    hdfs dfs -rm -r IndexInverter/out > /dev/null
fi
yarn jar $OUTPUT_DIR/$JAR_NAME $MAIN_CLASS_NAME -D mapred.reduce.tasks=$NO_REDUCERS IndexInverter/hortonworks.txt IndexInverter/out 

#listing the o/p file on hdfs
echo "Here are the output files..."
hdfs dfs -ls -R IndexInverter/out

