from resource_reader import ResourceReader

rr = ResourceReader()
config = rr.load_config()

ARN = config['IAM_ROLE']['ARN']
SONG_DATA = config['S3']['SONG_DATA']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSON_PATH']
REGION = config['AWS']['REGION']

# DROP TABLES
staging_events_table_drop = 'DROP TABLE IF EXISTS staging_event'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_song'
songplay_table_drop = 'DROP TABLE IF EXISTS songplay'
user_table_drop = 'DROP TABLE IF EXISTS users'
song_table_drop = 'DROP TABLE IF EXISTS song'
artist_table_drop = 'DROP TABLE IF EXISTS artist'
time_table_drop = 'DROP TABLE IF EXISTS time'


# CREATE TABLES
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastName  VARCHAR,
        length FLOAT, 
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR, 
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR NOT NULL,
        num_songs INTEGER,
        title VARCHAR,
        artist_id VARCHAR,
        artist_name VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        duration FLOAT,
        year INTEGER);
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER  IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR)

        DISTSTYLE KEY
        DISTKEY(start_time)
        SORTKEY(start_time);
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR,
        level VARCHAR)

        SORTKEY(user_id);
""")


song_table_create = (""" 
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR NOT NULL,
        song_title VARCHAR,
        year INTEGER,
        duration FLOAT,
        artist_id VARCHAR)
        
        SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL,
        artist_name VARCHAR,
        artist_location VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT)
        
        SORTKEY(artist_id);        
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour INTEGER,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        week INTEGER,
        weekday INTEGER)
        
        DISTSTYLE KEY
        DISTKEY(start_time)
        SORTKEY(start_time);
""")

# STAGING TABLES
# staging_events_copy = ("""
#     COPY staging_event
#     FROM {0}
#     IAM_ROLE = {1}
#     FORMAT AS JSON {2}
# """).format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSON_PATH"])

# staging_songs_copy = ("""
#     COPY staging_song
#     FROM {0}
#     IAM_ROLE = {1}'
#     FORMAT AS JSON 'auto'
# """).format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

staging_events_copy = ("""
    COPY staging_events
    FROM '{0}'
    CREDENTIALS 'aws_iam_role={1}'
    FORMAT AS JSON '{2}'
    STATUPDATE ON
    REGION '{3}';
""").format(LOG_DATA, ARN, LOG_JSON_PATH, REGION)

staging_songs_copy = ("""
    COPY staging_songs FROM '{0}'
    CREDENTIALS 'aws_iam_role={1}'
    FORMAT AS JSON 'auto'
    STATUPDATE ON
    REGION '{2}';
""").format(SONG_DATA, ARN, REGION)


# FINAL TABLES
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,  session_id, location, user_agent)                
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second', userId, 
        level, song_id, artist_id, sessionId, location, userAgent
    FROM staging_songs ss 
    JOIN staging_events se ON (ss.artist_name = se.artist AND ss.title = se.song)
    WHERE se.page = 'NextSong';
""")

# user_id, first_name, last_name, gender, level
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level 
    FROM staging_events
    WHERE page ='NextSong' AND userId IS NOT NULL;
""")

# song_id, song_title, year, duration, artist_id
song_table_insert = ("""
    INSERT INTO songs (song_id, song_title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

# artist_id, artist_name, artist_location, artist_latitude, artist_longitude
artist_table_insert = ("""
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

# start_time, hour, year, month, day, week, weekday
time_table_insert = ("""
    INSERT INTO time (start_time, hour, year, month, day, week, weekday)
    SELECT DISTINCT start_time, EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(YEAR FROM start_time) AS year, EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(DAY FROM start_time) AS day, EXTRACT(WEEK FROM start_time) AS week,
    EXTRACT(DOW FROM start_time) as weekday
    FROM songplays;
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, songplay_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
