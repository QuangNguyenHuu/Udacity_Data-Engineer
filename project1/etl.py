import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    To process data file as dataframe
    :param cur: cursor of active DB connect
    :param filepath: data file in json format which contains record data of artists, songs tables
    :return: None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    # song_data = df[song_table_columns].values[0].tolist()
    song_data = df[song_table_columns].values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    # artist_id, name, location, latitude, longitude
    artist_data = df[artist_columns].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    To process log file
    :param cur: cursor of active DB connect
    :param filepath: log file in json format that contains time, user, songplays table
    :return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page.eq('NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data record
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    # column_labels =
    time_df = pd.DataFrame(dict(zip(time_table_column_headers, time_data)))

    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[user_table_column_headers]

    # insert user data records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplays records
    for index, row in df.iterrows():
        # get song_id and artist_id from song and artist tables
        criteria = [row.artist, row.song, row.length]
        cur.execute(song_select, criteria)
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplays record
        songplay_data = ([pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, song_id, artist_id, row.sessionId,
                          row.location, row.userAgent])
        print(songplay_data)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    To process specific file @filepath
    :param cur: cursor of active DB connection
    :param conn: connection of DB
    :param filepath: path to file
    :param func: function of processing specific file contents
    :return: None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} file(s) found in "{}"'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
