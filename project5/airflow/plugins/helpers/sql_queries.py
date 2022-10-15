class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    migrate_query_json_template = 'COPY {0} FROM {1} ACCESS_KEY_ID {3} SECRET_ACCESS_KEY {4} JSON {5} COMPUPDATE OFF'
    migrate_query_csv_template = 'COPY {0} FROM {1} ACCESS_KEY_ID {2} SECRET_ACCESS_KEY {3} IGNOREHEADER {4} ' 'DELIMITER {5}'
    delete_query_template = 'TRUNCATE {0};'
    insert_query_template = 'INSERT INTO {0} {1};'
    count_query_template = 'SELECT COUNT(*) FROM {0};'
    select_query_template = 'SELECT * FROM {0};'
    sql_commit_query = 'COMMIT;'

    songplays_check_count_query = 'SELECT COUNT(*) FROM songplays;'
    users_check_count_query = 'SELECT COUNT(*) FROM users'
    songs_check_count_query = 'SELECT COUNT(*) FROM users'
    artists_check_count_query = 'SELECT COUNT(*) FROM users'
    time_check_count_query = 'SELECT COUNT(*) FROM users'

    songplays_data_quality_check_null = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE songplay_id IS NULL OR start_time IS NULL OR userid IS NULL;
    """)

    users_data_quality_check_null = ("""
        SELECT COUNT(*)
        FROM users
        WHERE userid IS NULL;
    """)

    songs_data_quality_check_null = ("""
        SELECT COUNT(*)
        FROM songs
        WHERE song_id IS NULL;
    """)

    artists_data_quality_check_null = ("""
        SELECT COUNT(*)
        FROM artists
        WHERE artist_id IS NULL;
    """)

    time_data_quality_check_null = ("""
        SELECT COUNT(*)
        FROM time
        WHERE start_time IS NULL;
    """)
