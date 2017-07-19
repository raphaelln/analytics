# analytics

This repository will contain some data analytics scenarios resolved using spark framework.

# Requirements
- cloudera distribution VM

# Scenario

## Top 3 cities with more accidents in Brazil between 2003 and 2007

The dataset was retrieve by **Public Brazilian Social Security** and shows the number of related accidents in all the cities of Brazil.

There are five kinds of accidents:
- Typical work accidents;
- Typical work route accidents;
- Accident by disease;
- Accident without official communication through the autorities;
- Death caused by work accidents

### Insights
- Top 3 cities with max number of accidents by state between 2003 and 2007 in Brazil.

### Result
- Results will be stored as parquet compressed files.
- Results will be stored as json uncompressed files.

### Files
- /accidents/retrieve.sh - Download the files from dataprev site and put on the hdfs
-/accidents/solution.py - Solution implemented with spark + spark Sql in python.

### Sample output
