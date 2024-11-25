#!/usr/bin/python

#create spark object
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()



spark.read.csv('file:///home/talentum/wh/whitehouse_visits.txt', header=False, inferSchema=True)\
    .filter(col('_c19') == "POTUS")\
    .select(col('_c0'), col('_c1'), col('_c6'), col('_c11'), col('_c21'), col('_c25'))\
    .withColumnRenamed('_c0', 'fname')\
    .withColumnRenamed('_c1', 'lname')\
    .withColumnRenamed('_c6', 'arrival_time')\
    .withColumnRenamed('_c11', 'appt_scheduled_time')\
    .withColumnRenamed('_c21', 'location')\
    .withColumnRenamed('_c25', 'comment')\
    .coalesce(1)\
    .write\
    .option('header', 'true')\
    .csv('file:///home/talentum/wh/potus')


