# Evaluation Lab 5 - Hive

## Inspect the data<br?
First we will ensure that the data is clean and consistent.
After having an overlook of the data, we can say that the data is clean and consistent.

## Problem with data<br>
The data does not contain any null values, but there is one problem with the data.
Some of the fields contain quotation marks(" ") for eg. ("Pune, Maharashtra").
There is a comma within the quotation marks which is the part of the value of the field.
But as we have defined the schema as `fields terminated by ','` hive will consider this comma as a separater between the two fields which is logically wrong.
To solve this issue we need to use the `OPENCSVserde` instead of `LazySimpleSerde`.

## Queries<br>
After solving the problems with loading the data into table we can query the data accordingly
