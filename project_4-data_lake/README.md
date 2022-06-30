# Project 4 : Data Lake

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project will build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

The project consists of the following files:
- etl.py: A Spark python script that runs the extraction and transformation pipeline
- dl.cfg: the AWS credentials for authentication


## Installation

Install the required modules by running 

```bash
$ init.sh
```

## Deployment

Configure dl.cfg.
Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with credentials that have read write access to S3.
INPUT_PATH should be set to s3a://udacity-dend/ for Udacity data set
OUTPUT_PATH should be set to a S3 bucket to which you have access.

Copy dl.cfg and etl.py to your EMR Cluster by running following commands:

```sh
scp -i <path to your SSH key> etl.py <your emr host>:~/
scp -i <path to your SSH key> dl.cfg <your emr host>:~/
```

## Execution

SSH to EMR cluster. Submit Apache Spark job from shell by running following command:

```sh
/usr/bin/spark-submit --master yarn etl.py