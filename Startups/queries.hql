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


--9)Find which state has the max no of startups

SELECT nullif(TRIM(split(location_of_company, ',')[1]), location_of_company), count(*) as count
FROM startups
WHERE TRIM(split(location_of_company, ',')[1]) IS NOT NULL
GROUP BY nullif(TRIM(split(location_of_company, ',')[1]), location_of_company)
ORDER BY count DESC
LIMIT 1;


--10) Find the total startups from 'Maharashtra'

--SELECT TRIM(split(Location_of_company, ',')[1]), COUNT(*) AS total_count
--FROM startups
--WHERE TRIM(split(Location_of_company, ',')[1]) = 'Maharashtra'
--GROUP BY TRIM(split(Location_of_company, ',')[1]);

SELECT COUNT(*) AS total_count
FROM startups
WHERE location_of_company LIKE '%Maharashtra%';


--11) How many startups were formed in 'Healthcare'

select count(*) 
from startups
where sector RLIKE 'Healthcare';








