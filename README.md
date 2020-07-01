# Project 3: Sparkify Data Warehouse
by **Adriano Vilela**

---


## Overview Project Sparkify DW
---

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project consists of consuming data that is available on Amazon S3, and loading the data into a structured Data Wharehouse in the AWS cloud using Redshift with PostgreSQL.

The analytics tables have been arranged in a star schema to allow the Sparkify team to readily run queries to analyze user activity on their app, such as on what songs users are listening to. The scripts have been created in Python.


## Data Schema
---

The fact and dimension tables have been defined for a schema that optimizes queries and analysis of music playback.

#### Fact Table

- **songplays** - records in log data associated with song plays i.e. records with page NextSong (columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

#### Dimension Tables
- **users** - users in the app (columns: user_id, first_name, last_name, gender, level)

- **songs** - songs in music database (columns: song_id, title, artist_id, year, duration)

- **artists** - artists in music database (columns: artist_id, name, location, lattitude, longitude)

- **time** - timestamps of records in songplays broken down into specific units (columns: start_time, hour, day, week, month, year, weekday)

#### Staging tables
- **staging_events** - event data telling what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)

- **staging_songs** - song data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)



## AWS Redshift set-up
---

AWS Redshift is used in ETL pipeline as the DB solution. Used set-up in the Project Sparkify is as follows:

**Cluster Config**:
- **Identifier:** sparkifyCluster
- **Type:** multi-node
- **Nodes:** 4x dc2.large nodes
- **IAM Role:** sparkifyRole
- **Location:** US-West-2 (as Project-3's AWS S3 bucket)



## Python scripts

---
Script execution sequence:
1. **create_tables.py:** This script clean previous schema and creates tables.

2. **etl.py:** This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into DB.



### RUN Sequence

**create_tables.py:**
- All tables are dropped.
- Create a new tables: Staging tables(staging_events,staging_songs), Dimensional tables(users, songs, artists, time) an fact table(songplays).
- Script writes to the list of dropped tables and the list of created tables, if an error occurs it presents the same.

**etl.py:**
- Script executes AWS Redshift COPY commands to insert source data (JSON files) to DB staging tables.
- From staging tables, data is further inserted to analytics tables.
- Script writes to console the query it's executing at any given time and if the query was successfully executed.


## Example queries
---

- TOP 10 Users more songs plays.

        SELECT  us.user_id,
                us.first_name,
                us.last_name,
                count(sp.songplay_id) AS Count_songplays
         FROM songplays AS sp
         JOIN users     AS us ON (us.user_id = sp.user_id)
        Group By 
                us.user_id,
                us.first_name,
                us.last_name
        ORDER BY (Count_songplays) DESC
        LIMIT 10;


**Result**:

user_id | first_name | last_name | count_songplays
--------|------------|-----------|----------------
49      |Chloe       |Cuevas     |2050
80      |Tegan       |Levine     |1952
15      |Lily        |Koch       |1352
29      |Jacqueline  |Lynch      |1048
88      |Mohammad    |Rodriguez  |870
97      |Kate        |Harrell    |829
36      |Matthew     |Jones      |666
16      |Rylan       |George     |594
85      |Kinsley     |Young      |578
44      |Aleena      |Kirby      |563


- TOP 10 Music more Songs player

        SELECT  so.title,
                ar.name As artist_name,
                so.year,
                count(sp.songplay_id) AS Count_songplays
         FROM songplays AS sp
         JOIN songs     AS so ON (so.song_id = sp.song_id)
         JOIN artists   As ar ON (ar.artist_id = so.artist_id)
        Group By 
                so.title,
                ar.name,
                so.year
        ORDER BY (Count_songplays) DESC
        LIMIT 10;

**Result**:

title                                        | artist_name   |year|count_songplays
---------------------------------------------|---------------|----|----------------
Speed Of Sound (Live)                        |Coldplay       |2005|58
One I Love                                   |Coldplay       |2002|58
Don't Panic                                  |Coldplay       |1999|58
A Rush Of Blood To The Head (Live In Sydney) |Coldplay       |2003|58
Day Old Blues                                |Kings Of Leon  |2004|55
Wicker Chair                                 |Kings Of Leon  |2003|55
Ragoo                                        |Kings Of Leon  |2007|55
Genius                                       |Kings Of Leon  |2003|55
You're The One                               |Dwight Yoakam  |1990|38
Black Mud                                    |The Black Keys |2010|36
