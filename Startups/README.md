# Evaluation Lab 5 - Hive

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
