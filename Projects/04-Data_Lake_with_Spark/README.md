# Data Lake Project

# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Description

In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.


# Running

Before running the ETL file `etl.py` it is mandatory to fulfill the `dl.cfg` with your AWS access key and secret key.

# ETL Pipeline

First the pipeline read the songs and logs data from S3.

```
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
```

Then it processes the data using spark to create the dimensions table (users, songs, artists, time) and the fact table (songplays). 

And finally it loads the data back to S3 within parquet files on S3.

# Discussion

This schema is really basic. But it answers, I think, all necessary questions the analytics team can have.

The Data Lake is appropriate in our case because we don't need to break down lot of informations in the dimension tables (like city coming from counties coming from countries, ...). And with this schema, the analytics team has a clear view on how to request data and aggregate if needed. NO complexity here with basic analytic knowledges.

So I think I correctly answer the demand of the Sparkify team to build a Data Lake on AWS using the data store in S3 buckets.