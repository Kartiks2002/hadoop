# IndexInverter Job Automation Script

## Overview

This script automates the process of compiling, creating a JAR file, uploading input data to HDFS, and running a Hadoop MapReduce job on YARN. It is designed to process the `hortonworks.txt` input file and execute the `IndexInverterJob` in a Hadoop environment.

## Requirements

- Place this script in the same directory as the Java source files and the `hortonworks.txt` file.
- Ensure you have access to a Hadoop/YARN cluster.

## Script Functionality

The script will:
1. Compile the Java source files.
2. Create a `build` directory in the current directory.
3. Upload `hortonworks.txt` to HDFS.
4. Generate a JAR file for the job.
5. Run the MapReduce job on YARN.

## Usage

To run the script, use the following command:

```bash
./script.sh [--reducers NUM_OF_REDUCERS]
