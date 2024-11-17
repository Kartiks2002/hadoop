#!/bin/bash

#Prerequisite: The .java and hortonworks.txt files should be in the same directory as this script.

# Author: Kartik
# Created on: 11th Nov 2024
# Last Modified: 15th Nov 2024

# Purpose: Export the IndexInverter Project to a jar file and run on YARN.

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
LOG_FILE="/tmp/index_inverter_job.log"

display_help() {
    echo -e "${GREEN}IndexInverter Job Automation Script${NC}"
    echo "Usage: ./script.sh [--reducers NUM_OF_REDUCERS]"
    echo
    echo "This script should be placed in the same directory as the Java source files and hortonworks.txt."
    echo "It will compile the Java files, create a build directory in the current directory,"
    echo "upload hortonworks.txt to HDFS, create a JAR file, and run it on YARN."
    echo
    echo "Parameters:"
    echo "  NUM_OF_REDUCERS (optional): Number of reducers. Default is 1."
    echo
    echo "Prerequisite: The .java and hortonworks.txt files should be in the same directory as this script."
    echo
    exit 0
}

# Check for --help argument
if [[ "$1" == "--help" ]]; then
    display_help
fi

# Initialize the number of reducers to the default value
NUM_OF_REDUCERS=1

# Process command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --reducers)
            NUM_OF_REDUCERS="$2"  # Set number of reducers to the next argument
            shift 2               # Shift to the next argument
            ;;
        *)
            echo -e "${RED}Error: Invalid option $1${NC}"
            display_help
            ;;
    esac
done


# Set variables
#NUM_OF_REDUCERS=${1:-1}
SCRIPT_DIR=$(dirname "$0")  # Directory where the script is located
SRC_DIR="$SCRIPT_DIR"       # Use the script's directory as the source directory
OUTPUT_DIR="$SCRIPT_DIR/build"  # Build directory in the same location as the script
MAIN_CLASS_NAME="IndexInverterJob"  # Assuming your main class is named IndexInverterJob
TEXT_FILE="hortonworks.txt"
JAR_NAME="IndexInverterJob.jar" 

# Check if hortonworks.txt exists in the same directory as the script
if [ ! -f "$SCRIPT_DIR/$TEXT_FILE" ]; then
    echo -e "${RED}Error: $TEXT_FILE not found in the same directory as the script.${NC}"
    exit 1
fi

# Check if IndexInverterJob.java exists in the same directory as the script
if [ ! -f "$SCRIPT_DIR/IndexInverterJob.java" ]; then
    echo -e "${RED}Error: IndexInverterJob.java not found in the same directory as the script.${NC}"
    exit 1
fi

# Create output/build directory if it doesn't exist
mkdir -p $OUTPUT_DIR

# Start logging
echo "Job started at $(date)" >> $LOG_FILE

# Check if the IndexInverter folder exists in HDFS, if yes, clear it
hdfs dfs -test -d IndexInverter
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Clearing existing IndexInverter directory in HDFS...${NC}"
    hdfs dfs -rm -r IndexInverter >> $LOG_FILE 2>&1
fi

# Create IndexInverter directory in HDFS
echo -e "${GREEN}Creating IndexInverter directory in HDFS...${NC}"
hdfs dfs -mkdir -p IndexInverter >> $LOG_FILE 2>&1

# Upload the hortonworks.txt file to HDFS
echo -e "${GREEN}Uploading $TEXT_FILE to HDFS...${NC}"
hdfs dfs -put "$SCRIPT_DIR/$TEXT_FILE" IndexInverter/ >> $LOG_FILE 2>&1
if [ $? -ne 0 ]; then
    echo "Error uploading $TEXT_FILE to HDFS. Check the log at $LOG_FILE."
    exit 1
fi

# Compile Java source files (no package structure)
echo -e "${GREEN}Compiling Java files in directory $SRC_DIR...${NC}"
javac -classpath "$(hadoop classpath)" -d $OUTPUT_DIR $SRC_DIR/*.java 2>> $LOG_FILE

# Check for errors during compilation
if [ $? -ne 0 ]; then
    echo -e "${RED}Compilation failed. Please check the error details in the log file $LOG_FILE.${NC}"
    tail -n 20 $LOG_FILE  # Display last 20 lines of the log file
    exit 1
fi

# Create JAR file
echo -e "${GREEN}Creating JAR file in $OUTPUT_DIR...${NC}"
jar -cvf $OUTPUT_DIR/$JAR_NAME -C $OUTPUT_DIR/ . >> $LOG_FILE 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}JAR creation failed. Check the log at $LOG_FILE.${NC}"
    exit 1
fi

# Check and remove existing output directory in HDFS
hdfs dfs -test -d IndexInverter/out
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Removing existing output directory in HDFS...${NC}"
    hdfs dfs -rm -r IndexInverter/out >> $LOG_FILE 2>&1
fi

# Run the JAR file using YARN
echo -e "${GREEN}Running the application...${NC}"
yarn jar $OUTPUT_DIR/$JAR_NAME $MAIN_CLASS_NAME -D mapreduce.job.reduces=$NUM_OF_REDUCERS IndexInverter/$TEXT_FILE IndexInverter/out >> $LOG_FILE 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}YARN job failed. Check the log at $LOG_FILE.${NC}"
    exit 1
fi

# Display output files
echo -e "${GREEN}Job completed successfully. Here are the output files:${NC}"
hdfs dfs -ls -R IndexInverter/out
