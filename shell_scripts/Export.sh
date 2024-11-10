#!/bin/bash

# Author: Kartik
# Created on: 10th Nov 2024 (23:53)
# Last modified: 10th Nov 2024 

#Purpose: Export the java project and specify the no of reducers and run the project 

if [ $# -lt 4  ]; then
	echo "Usage: $0 <JavaPackageName> <MainClassName> <InputPath> <OutputPath> <NumReducers>"
  exit 1
fi 

JavaPackageName=$1 
MainClassName=$2
InputPath=$3
OutputPath=$4
NumReducers=$5

PROJECT_DIR="/home/cloudera/workspace/WordCount/"

#compiling the java files
javac -classpath $(hadoop classpath) -d $PROJECT_DIR/bin/wordcount $PROJECT_DIR/src/wordcount/*.java

if [ $? -ne 0 ]; then
	echo "Java compilation failed"
	exit 1
fi

#creating the jar file
echo "Creating JAR file..."
jar -cvf $PROJECT_DIR/src/wordcount.jar -C $PROJECT_DIR/bin .

if [ $? -ne 0 ]; then
        echo "Failed to create jar file"
        exit 1
fi

echo "Running the job with $NUMREDUCERS recuders"
yarn jar $PROJECT_DIR/src/wordcount.jar $JavaPackageName.$MainClassName -D mapreduce.job.reduces=$NumReducers $InputPath $OutputPath


if [ $? -ne 0 ]; then
  echo "Hadoop job submission failed."
  exit 1
fi
echo "Hadoop job successfully submitted."


