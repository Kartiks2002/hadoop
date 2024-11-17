# Hadoop Ecosystem Automation Scripts

This repository contains two scripts to automate the starting and stopping of the Hadoop ecosystem (HDFS and YARN).

## Scripts

- **start_hadoop.sh**  
  Starts the Hadoop ecosystem by:
  - Formatting the NameNode (only on first run).
  - Starting HDFS and YARN services.
  - Creating the user directory in HDFS.
  - Displaying running processes with `jps`.

- **stop_hadoop.sh**  
  Stops the Hadoop ecosystem by:
  - Stopping HDFS and YARN services.
  - Displaying running processes with `jps`.

## Usage

**Start Hadoop Ecosystem**:
   chmod +x start_hadoop.sh
   ./start_hadoop.sh
