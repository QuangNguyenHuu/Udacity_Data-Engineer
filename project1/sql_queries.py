# DROP TABLES
# songplay_table_drop = ""
# user_table_drop = ""
# song_table_drop = ""
# artist_table_drop = ""
# time_table_drop = ""

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Fact Table
# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT NOT NULL,
        level VARCHAR(4),
        song_id VARCHAR(18),
        artist_id VARCHAR(18),
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR,
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")

# Dimension Tables
# users - users in the app
# user_id, first_name, last_name, gender, level
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY NOT NULL,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR(4) NOT NULL
    );
""")

# songs - songs in music database
# song_id, title, artist_id, year, duration
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(18) PRIMARY KEY NOT NULL,
        title VARCHAR NOT NULL,
        artist_id VARCHAR(18),
        year INT NOT NULL,
        duration FLOAT NOT NULL
    );
""")

# artists - artists in music database
# artist_id, name, location, latitude, longitude
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(18) PRIMARY KEY NOT NULL,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    );
""")

# time - timestamps of records in songplays broken down into specific units
# Extract the timestamp, hour, day, week of year, month, year, and weekday from the ts column and set time_data to a list containing these values in order
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY NOT NULL,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, year, month, day, hour, week, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
song_select = ("""
    SELECT s.song_id, a.artist_id  
    FROM songs s
    JOIN artists a ON s.artist_id = a.artist_id
    WHERE (a.name = %s OR s.title = %s) AND s.duration = %s
    AND a.name IS NOT NULL AND s.artist_id IS NOT NULL;
""")

# kill active connection to current DB
backend_pip_select = ("""
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE datname = current_database()
    AND pid <> pg_backend_pid();
""")

# Header of tables
song_column_header = ['song_id', 'title', 'artist_id', 'year', 'duration']
artist_column_header = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
time_table_column_headers = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
user_table_column_headers = ['userId', 'firstName', 'lastName', 'gender', 'level']

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
song_table_columns = song_column_header
artist_columns = artist_column_header
time_table_columns = [time_table_column_headers]
user_table_columns = [user_table_column_headers]
