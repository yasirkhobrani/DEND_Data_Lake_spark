# Sparkify data lake with spark
## About Sparkify
Sparkify is a music startup, they store songs data and their logs in separate JSON files, analyzing those data becomes diffcult, in order to handle those large data, they need to process and extract data using ETL pipelines, then store it in the destination database.



# Database Schema
## Sparkify destination database will have the following tables:

Songplays (The fact table)
Songs (Dimensional table from song_data json files)
Artists (Dimensional table from song_data json files)
Users (Dimensional table from log_data json files)
Time (Dimensional table from Timestamp Column)


## ETL Pipline
Extract Transform and Load data:
* Extracting data from json files (s3 bucket).
* Transforming data into tables.
* Load data to s3 bucket.

## Loaded tables (partitioned) 
- Songs table partitioned by "year" and "artist_id".
- Time table partitioned by "year" and "month".
- Songplays table partitioned by "year" and "month".

## How to run:
1. Run the terminal.
2. Execute etl.py
