# Custom Spark Datasource

This repository contains a sample Spark application that implements the Datasource API. 
For simplicity's sake, the implementation works with text files that have three columns separated by "$",
which include information about name, surname, salary.
 
## Schema Identification
 
1. Create a class called `DefaultSource` that extends `RelationProvider` and `SchemaRelationProvider` traits.
 
The `RelationProvider` trait is implemented by objects that produce relations for a specific kind of 
data source. Users may omit the fully qualified class name of a given data source. In this case, 
Spark SQL will append the class name `DefaultSource` to the path, 
allowing for less verbose invocation.  For example, `org.apache.spark.sql.json` would resolve to
the data source `org.apache.spark.sql.json.DefaultSource`. 
 
2. Create a class that extends `BaseRelation`. 
  
`BaseRelation` represents a collection of tuples with a known schema. 
Simply speaking, it is used to infer/define schemas.
 
## Reading Data
 
1. Implement the `TableScan` trait in the custom relation class. This method should return all rows
from the custom data source as an `RDD` of `Rows`.

## Writing Data

1. To support write calls, `DefaultSource` has to implement one additional trait called `CreatableRelationProvider`.

## Column Pruning

1. To implement the column pruning, the custom relation class has to implement the `PrunedScan`. It can help to optimize the column access.

## Filter Pushdown

1. To optimize filtering, the custom relation class can extend the `PrunedFilterScan` trait.