import os
import csv
import glob
from create_tables import *


def process_event_data():
    """
    Creating list of file_path to process original event csv data files
    Browsing & processing whole csv files in event_folder
    Processing the files to create the data file csv that will be used for Apache Casssandra tables
    """
    file_path_list = []
    # checking your current working directory
    # print(os.getcwd())
    # Get your current folder and subfolder event data
    file_path = os.getcwd() + event_data_folder

    # creating "processed_event_data_file.csv" as new processed file
    processed_file_path = file_path + new_event_csv_file
    if os.path.exists(processed_file_path):
        os.remove(processed_file_path)
        print('Delete file: {}'.format(new_event_csv_file))

    # Create a for loop to create a list of files and collect each file_path
    # join the file path and roots with the subdirectories using glob
    for root, dirs, files in os.walk(file_path):
        file_path_list = glob.glob(os.path.join(root, '*'))
        # print(file_path_list)

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []
    # for every filepath in the file path list
    for f in file_path_list:
        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csv_file:
            # creating a csv reader object
            csv_reader = csv.reader(csv_file)
            next(csv_reader)
            # extracting each data row one by one and append it
            for line in csv_reader:
                # print(line)
                full_data_rows_list.append(line)

    # uncomment the code below if you would like to get total number of rows
    # print(len(full_data_rows_list))
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    # print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(processed_file_path, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'first_name', 'last_name', 'gender', 'item_in_session', 'length', 'level', 'location',
                         'session_id', 'song_title', 'user_id'])
        for row in full_data_rows_list:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[5], row[3], row[4], row[6], row[7], row[8], row[12], row[13], row[16]))

    # check the number of rows in your csv file
    with open(processed_file_path, 'r', encoding='utf8') as f:
        print('Rows: {}'.format(sum(1 for line in f)))
    print('{} created and event data processed completed'.format(new_event_csv_file))
    return processed_file_path


def import_event_data(session, file_path):
    """
    To process csv file @file_path by inserting song data into Cassandra
    Cassandra tables: song_table, artist_table, user_table.
    :param session: session of Cassandra
    :param file_path: path of processing file in ".csv" format
    :return: None
    """
    # We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
    with open(file_path, 'r', encoding='utf8') as f:
        total_row_number = sum(1 for line in f)

    with open(file_path, encoding='utf8') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # skip header
        for i, line in enumerate(csv_reader, 2):
            # print('Artist 0: {}'.format(line[0]))
            # print('First Name 1: {}'.format(line[1]))
            # print('last Name 2: {}'.format(line[2]))
            # print('Gender 3: {}'.format(line[3]))
            # print('Item In Session 4: {}'.format(line[4]))
            # print('Length 5: {}'.format(line[5]))
            # print('Level 6: {}'.format(line[6]))
            # print('Location 7: {}'.format(line[7]))
            # print('Session ID 8: {}'.format(line[8]))
            # print('Song Title 9: {}'.format(line[9]))
            # print('User ID 10: {}'.format(line[10]))

            # Assign the INSERT statements into the `query` variable

            # user_id, first_name, last_name, gender, song_title, location
            statement = session.prepare(users_table_insert)
            data = [int(line[10]), line[1], line[2], line[3], line[9], line[7]]
            session.execute(statement, data)

            # song_title, artist, length, session_id, item_in_session
            statement = session.prepare(songs_table_insert)
            data = [line[9], line[0], float(line[5]), int(line[8]), int(line[4])]
            session.execute(statement, data)

            # artist, item_in_session, user_id, session_id
            statement = session.prepare(artists_table_insert)
            data = [line[0], int(line[4]), int(line[10]), int(line[8])]
            session.execute(statement, data)
            print('Processing row {}/{} completed'.format(i, total_row_number))

    print('DONE')


def main():
    # Create cassandra connection
    keyspace_name = 'sparkify_test2'
    cluster = create_cluster()
    session = create_session(keyspace_name)

    drop_tables(session)
    create_tables(session)
    # Create new csv file by processing data from event data files
    new_file_path = process_event_data()

    # insert record
    import_event_data(session, new_file_path)
    results = execute_query(session, select_song_query)
    for r in results:
        print(r)
        # print(r.artist, r.song_title, r.length)
    execute_queries(session, find_song_queries)
    stop_session(session)
    stop_cluster(cluster)


if __name__ == "__main__":
    main()
