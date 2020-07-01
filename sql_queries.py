import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS song;"
artist_table_drop         = "DROP TABLE IF EXISTS artist;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
            event_id    BIGINT IDENTITY(0,1)    NOT NULL,
            artist      VARCHAR                 ,
            auth        VARCHAR                 ,
            firstName   VARCHAR                 ,
            gender      VARCHAR                 ,
            itemInSession VARCHAR               ,
            lastName    VARCHAR                 ,
            length      VARCHAR                 ,
            level       VARCHAR                 ,
            location    VARCHAR                 ,
            method      VARCHAR                 ,
            page        VARCHAR                 ,
            registration VARCHAR                ,
            sessionId   INTEGER                 NOT NULL SORTKEY DISTKEY,
            song        VARCHAR                 ,
            status      INTEGER                 ,
            ts          BIGINT                  NOT NULL,
            userAgent   VARCHAR                 ,
            userId      INTEGER                 
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs           INTEGER         ,
            artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,
            artist_latitude     VARCHAR         ,
            artist_longitude    VARCHAR         ,
            artist_location     VARCHAR(500)    ,
            artist_name         VARCHAR(500)    ,
            song_id             VARCHAR         NOT NULL,
            title               VARCHAR(500)    ,
            duration            DECIMAL(9)      ,
            year                INTEGER         
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
            songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
            start_time  TIMESTAMP               NOT NULL,
            user_id     VARCHAR(50)             NOT NULL DISTKEY,
            level       VARCHAR(10)             NOT NULL,
            song_id     VARCHAR(40)             NOT NULL,
            artist_id   VARCHAR(50)             NOT NULL,
            session_id  VARCHAR(50)             NOT NULL,
            location    VARCHAR(100)            ,
            user_agent  VARCHAR(255)            
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER                 NOT NULL SORTKEY,
            first_name  VARCHAR(50)             ,
            last_name   VARCHAR(80)             ,
            gender      VARCHAR(10)             ,
            level       VARCHAR(10)             
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
            song_id     VARCHAR(50)             NOT NULL SORTKEY,
            title       VARCHAR(500)            NOT NULL,
            artist_id   VARCHAR(50)             NOT NULL,
            year        INTEGER                 NOT NULL,
            duration    DECIMAL(9)              NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
            artist_id   VARCHAR(50)            NOT NULL SORTKEY,
            name        VARCHAR(500)           ,
            location    VARCHAR(500)           ,
            latitude    DECIMAL(9)             ,
            longitude   DECIMAL(9)             
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
            start_time  TIMESTAMP               NOT NULL SORTKEY,
            hour        SMALLINT                ,
            day         SMALLINT                ,
            week        SMALLINT                ,
            month       SMALLINT                ,
            year        SMALLINT                ,
            weekday     SMALLINT                
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,
                           user_id,
                           level,
                           song_id,
                           artist_id,
                           session_id,
                           location,
                           user_agent)
    SELECT  DISTINCT 
            TIMESTAMP 'epoch' + event.ts/1000 
            * INTERVAL '1 second'       AS start_time,
            event.userid                AS user_id,
            event.level                 AS level,
            song.song_id               AS song_id,
            song.artist_id             AS artist_id,
            event.sessionId             AS session_id,
            event.location              AS location,
            event.userAgent             AS user_agent
    FROM staging_events AS event
    JOIN staging_songs  AS song
        ON (event.artist = song.artist_name)
    WHERE event.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
                       first_name,
                       last_name,
                       gender,
                       level)
    SELECT  DISTINCT 
         userId          AS user_id,
         firstName       AS first_name,
         lastName        AS last_name,
         gender          AS gender,
         level           AS level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,
                       title,
                       artist_id,
                       year,
                       duration)
    SELECT DISTINCT 
         song_id         AS song_id,
         title           AS title,
         artist_id       AS artist_id,
         year            AS year,
         duration        AS duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                         name,
                         location,
                         latitude,
                         longitude)
    SELECT  DISTINCT
         artist_id          AS artist_id,
         artist_name        AS name,
         artist_location    AS location,
         artist_latitude    AS latitude,
         artist_longitude   AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time,
                      hour,
                      day,
                      week,
                      month,
                      year,
                      weekday)
    SELECT  DISTINCT 
            TIMESTAMP 'epoch' + ts/1000 \
                * INTERVAL '1 second'         AS start_time,
            EXTRACT(hour  FROM start_time)    AS hour,
            EXTRACT(day   FROM start_time)    AS day,
            EXTRACT(week  FROM start_time)    AS week,
            EXTRACT(month FROM start_time)    AS month,
            EXTRACT(year  FROM start_time)    AS year,
            EXTRACT(week  FROM start_time)    AS weekday
    FROM    staging_events 
    WHERE page = 'NextSong';
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
