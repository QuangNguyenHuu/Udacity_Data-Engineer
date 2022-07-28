# DATA INFO
# event_datafile_new.csv
new_event_csv_file = '/processed_event_data_file.csv'
event_data_folder = '/event_data'

# DROP TABLES

# add IF EXIST following as Project Rubric
delete_songs_table = "DROP TABLE IF EXISTS songs"
delete_artists_table = "DROP TABLE IF EXISTS artists"
delete_users_table = "DROP TABLE IF EXISTS users"


# CREATE TABLES
create_songs_table = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_title VARCHAR,
    artist VARCHAR,
    length FLOAT,
    session_id BIGINT, 
    item_in_session BIGINT, 
    PRIMARY KEY (session_id, item_in_session));
""")

create_artists_table = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist VARCHAR,
    item_in_session BIGINT,
    user_id BIGINT,
    session_id BIGINT,
    PRIMARY KEY ((user_id, session_id), item_in_session));
""")

#  user_id, first_name, last_name, gender, song_title, location
create_users_table = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT, 
    first_name VARCHAR, 
    last_name VARCHAR,
    gender VARCHAR,
    song_title VARCHAR,
    location VARCHAR,
    PRIMARY KEY ((song_title), user_id));
""")


# QUERY LISTS
select_song_query = ("""
    SELECT song_title, artist, length
    FROM songs 
    WHERE session_id = 338 AND item_in_session = 4;
""")

select_artist_query = ("""
    SELECT artist, item_in_session
    FROM artists 
    WHERE user_id = 10 AND session_id = 182; 
""")

select_user_query = ("""
    SELECT first_name, last_name, gender, song_title, location
    FROM users
    WHERE song_title = 'All Hands Against His Own';
""")

# song_title, artist, length, session_id, item_in_session
songs_table_insert = ("""
    INSERT INTO songs (song_title, artist, length, session_id, item_in_session)
    VALUES (?,?,?,?,?);
""")

# artist, item_in_session, user_id, session_id
artists_table_insert = ("""
    INSERT INTO artists (artist, item_in_session, user_id, session_id)
    VALUES (?,?,?,?);
""")

# user_id, first_name, last_name, gender, song_title, location
users_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, song_title, location)
    VALUES (?,?,?,?,?,?);
""")

drop_table_queries = [delete_songs_table, delete_artists_table, delete_users_table]
create_table_queries = [create_songs_table, create_artists_table, create_users_table]
find_song_queries = [select_song_query, select_artist_query, select_user_query]
