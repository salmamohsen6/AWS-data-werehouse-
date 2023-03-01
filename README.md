# project

# files 
there are 4 files

create_tables.py : creates and drop tables 

dwh.cfg : Configuration of Redshift cluster and data 

etl.py Copy data to staging tables and insert into our schema 

sql_queries.py:  contains sql quiers to : 
1)Create and drop staging and star schema tables
2)Copy JSON data from S3 to Redshift staging tables
3)Insert data from staging tables to tables

# how to run code

