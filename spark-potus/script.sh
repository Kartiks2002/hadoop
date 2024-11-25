#!/bin/bash

# Author: Kartik
# Created on: 25-11-2024 (23:37)
# Last Modified on: 25-11-2024

hdfs dfs -test -f whitehouse_visits.txt

if [ $? -eq 0 ];then
        rm whitehouse_visits.txt
        hdfs dfs -copyFromLocal whitehouse_visits.txt
fi

python extract_potus.py
