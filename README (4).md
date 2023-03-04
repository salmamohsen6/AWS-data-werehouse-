# project
To facilitate data analysis for Sparkify,using data engineering to develop an ETL pipeline that extracts their data from S3, stages it in Redshift, and transforms it into a set of dimensional tables. The objective of this project is to create a data warehouse on the cloud (AWS Redshift) and build an ETL pipeline for analysis.

technologies:
sql
Python
AWS (s3, IAM(user +role), ec2(security group),vpc, redshift(cluster))

# files 
there are 4 files

create_tables.py : creates and drop tables 

dwh.cfg : Configuration of Redshift cluster and data 

etl.py Copy data to staging tables and insert into our schema 

sql_queries.py:  contains sql quiers to : 
1)Create and drop staging and star schema tables
2)Copy JSON data from S3 to Redshift staging tables
3)Insert data from staging tables to tables
# schema 
we are using star scehma consiting of a fact table and 4 dimentional tables

### fact table
songplays {songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent}.

### dimentional tables
users {user_id, first_name, last_name, gender, level}

songs  {song_id, title, artist_id, year, duration}

artists {artist_id, name, location, lattitude, longitude}

time {start_time, hour, day, week, month, year, weekday}


The data stored on S3 bucket is copied to staging tables on Redshift cluster. Then the data from these tables are transformed and inserted into our schema

# how to run code

frist create you AWS redshift cluster then edit the configration in the dwh.cfg

second in terminal run 

```
python create_tables.py
```
then

```
python etl.py
```
