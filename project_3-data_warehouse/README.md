# Project 3: Data warehouse

Project submission for Udacity Data Engineering Nanodegree - Data Warehouse

## Summary
This project combines song listen log files with song metadata to facilitate analytics. 
JSON data is copied from an S3 bucket to Redshift staging tables before being inserted into a star schema with fact and dimension tables. 
Analytics queries are run on the ` songplays` fact table and additional fields can be easily accessed in the four dimension tables `users`, `songs`, `artists`, and `time`. 
A star schema is suitable for this application since de normalization is easy, queries can be kept simple, and aggregations are fast.

## Prerequisite
A Redshift cluster should be  created using AWS console

## Installation

```bash
$ init.sh
```

## Files

**`init.sh`** executes pip command to load modules in `requirements.txt`

**`dwh.cfg`** environment properties

**`create_tables.py`** Drop and recreate tables

**`etl.py`** Copy data to staging tables and insert into star schema fact and dimension tables

**`sql_queries.py`**

* Creating and dropping staging and star schema tables
* Copy JSON data from S3 to Redshift staging tables
* Insert data from staging tables to star schema fact and dimension tables

## Run scripts

Set environment variables in `dwh.cfg`

Drop and recreate tables

```bash
$ python create_tables.py
```

Run ETL pipeline

```bash
$ python etl.py
```
