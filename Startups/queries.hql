-- drop the table if already exists and create a new one
drop table if exists startups;

CREATE TABLE startups (
    Incubation_Center STRING,
    Name_of_startup STRING,
    Location_of_company STRING,
    Sector STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
TBLPROPERTIES ('skip.header.line.count'='1');

-- load the data from hdfs
LOAD DATA INPATH '/user/cloudera/Listofstartups.csv'
INTO TABLE startups;

-- Q1 Find which sector has the most startups?

select sector, COUNT(1) as num_startups
     from startups2
     group by sector
     order by num_startups desc
     limit 1;

