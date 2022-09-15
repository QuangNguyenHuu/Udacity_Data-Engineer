# VIEW TEMPLATE
default_view_template = 'view_template'

# CREATE TABLES
songs_table_sql = ("""
    SELECT song_id, title, duration, year, artist_id
    FROM view_template
    ORDER BY song_id
""")

artists_table_sql = ("""
    SELECT  artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM view_template
    ORDER BY artist_id desc
""")

songplays_table_sql = ("""
    SELECT nd.songplay_id, sd.song_id, nd.time_stamp, nd.level,
           sd.artist_id, nd.sessionId AS session_id, 
           nd.userId AS user_id, nd.location, nd.userAgent AS user_agent 
    FROM song_data_view sd
    INNER JOIN nextsong_data_view nd ON sd.artist_name == nd.artist AND sd.title == nd.song
""")

users_table_sql = ("""
    SELECT  DISTINCT userId AS user_id, firstName AS first_name, lastName  AS last_name, gender, level
    FROM view_template
    ORDER BY first_name, last_name
""")

time_table_sql = ("""
    SELECT DISTINCT time_stamp, year(time_stamp) AS year, month(time_stamp) AS month, day(time_stamp) AS day,
                     hour(time_stamp) AS hour, weekofyear(time_stamp) AS week, dayofweek(time_stamp) AS weekday
    FROM view_template
    ORDER BY (year, month)
""")


# QUERY TEMPLATE LIST 
songs_table_query_template = songs_table_sql
artists_table_query_template = artists_table_sql
songplays_table_query_template = songplays_table_sql
users_table_query_template = users_table_sql
time_table_query_template = time_table_sql
