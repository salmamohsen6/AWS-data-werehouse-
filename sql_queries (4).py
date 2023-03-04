import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')
ARN=config.get('IAM_ROLE','ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events (
  artist varchar,
  auth varchar,
  firstName varchar,
  gender char(1),
  itemInSession int,
  lastName varchar,
  length float,
  level varchar,
  location text,
  method varchar,
  page varchar,
  registration varchar,
  sessionId int,
  song varchar,
  status int,
  ts bigint,
  userAgent text,
  userId int
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
num_songs int,
artist_id varchar, 
artist_latitude float ,
artist_longitude float ,
artist_location varchar, 
artist_name varchar, 
song_id  varchar,
title varchar,
duration float,
year int
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay(
songplay_id int IDENTITY (0,1),
start_time timestamp,
user_id int, 
level varchar, 
song_id varchar,
artist_id varchar,
session_id varchar,
location varchar,
user_agent varchar
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users(
user_id varchar NOT NULL PRIMARY KEY ,
first_name varchar,
last_name varchar,
gender char(1),
level varchar
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS song(
song_id varchar NOT NULL PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration float)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artist(
artist_id varchar NOT NULL PRIMARY KEY,
name varchar,
location varchar,
lattitude float,
longitude float
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
start_time timestamp NOT NULL PRIMARY KEY,
hour int, 
day int,
week int,
month int,
year int,
weekday int
)
"""

# STAGING TABLES

staging_events_copy = ("""
copy  staging_events
from {}
credentials 'aws_iam_role={}' 
region 'us-west-2'
json{}
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs
from{}
credentials 'aws_iam_role={}' 
region 'us-west-2'
json 'auto'
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = """
insert into songplay(start_time ,user_id , level , song_id ,artist_id ,session_id ,location ,user_agent)
select distinct  timestamp 'epoch' + Se.ts *  interval '1 second',
            Se.userId,
            Se.level,
            SS.song_id ,
            Ss.artist_id,
            Se.sessionId ,
            Se.location ,
            Se.userAgent
        
from staging_events Se
inner join staging_songs Ss
on Se.song=Ss.title and Se.length=Ss.duration and Se.artist=Ss.artist_name
where Se.page='NextSong' 
        
        
        
"""

user_table_insert = """insert into users(user_id ,first_name ,last_name ,gender,level)
select distinct userId,
    firstName,
    lastName,
    gender,
    level
from staging_events 
WHERE page = 'NextSong' 
"""

song_table_insert = """insert into song(song_id ,title,artist_id ,year ,duration)
select distinct song_id,
                title,
                artist_id,
                year,
                duration
from staging_songs
"""

artist_table_insert = """insert into artist(artist_id ,name ,location ,lattitude ,longitude)
select distinct artist_id ,
               artist_name,
               artist_location,
               artist_latitude,
               artist_longitude
                
from staging_songs
              
                
"""

time_table_insert = """insert into time(start_time,hour, day,week,month,year,weekday)
select distinct start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(weekday from start_time)
from songplay
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]