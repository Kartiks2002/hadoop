# Evaluation Lab 5 - Hive

This script automates uploading a CSV file to HDFS, creating a Hive table, and running a query to analyze startup data.

Prerequisites:
- Hadoop & Hive installed and running.
- HDFS access and permission to read/write.
- `Listofstartups.csv` file in the same directory as the script.

Files:
- `Listofstartups.csv`: Startup data.
- `queries.hql`: Hive queries (create table, load data, run analysis).
- `script.sh`: Automation script.

Setup & Run:
1. Make the script executable:
   chmod +x automation_script.sh

2. Run the script:
   ./automation_script.sh

What the Script Does:
1. Removes any existing `Listofstartups.csv` from HDFS.
2. Uploads the CSV file to HDFS.
3. Executes Hive queries to:
   - Create the table.
   - Load data from CSV.
   - Find the sector with the most startups.

## Inspect the data<br>
First we will ensure that the data is clean and consistent.
After taking a look the data, we can say that the data is clean and consistent.

## Problem with data<br>
The data does not contain any null values, but there is one issue.
Some of the fields contain quotation marks(" ") for example `("Pune, Maharashtra")`.
There is a comma within the quotation marks that is the part of the value of the field.
Since we have defined the schema as `fields terminated by ','`, Hive will consider this comma as a separater between the two fields which is logically wrong.
To solve this issue we need to use the `OPENCSVserde` instead of `LazySimpleSerde`.

## Queries<br>
After solving the problems with loading the data into table we can query the data accordingly
