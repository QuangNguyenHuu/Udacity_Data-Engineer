import psycopg2

from resource_reader import ResourceReader
from sql_queries import copy_table_queries, insert_table_queries


def migrate_staging_table_data(cur, conn):
    """
    Migrates staging event & staging song data from S3 to Redshift
    :param cur: cursor of connection to connected database.
    :param conn: connection of database
    :return: None
    """
    for query in copy_table_queries:
        print('Migrate staging data. Query [{}]'.format(query))
        cur.execute(query)
        conn.commit()
    print('Staging data migrated. DONE!')


def insert_analytical_table_data(cur, conn):
    """
    Inserts analytical data into songplay, users, song, artist, time tables
    :param cur: cursor of connection to connected database.
    :param conn: connection of database
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print('Insert data. Query [{}]'.format(query))
    print('Insert data completed. DONE!')


def main():
    rr = ResourceReader()
    config = rr.load_config()

    conn_string = "host={} dbname={} user={} password={} port={}".format(
        config["CLUSTER"]['HOST'], config['CLUSTER']['DB_NAME'], config['CLUSTER']['DB_USER'],
        config['CLUSTER']['DB_PASSWORD'], config['CLUSTER']['DB_PORT'])
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    migrate_staging_table_data(cur, conn)
    insert_analytical_table_data(cur, conn)


if __name__ == "__main__":
    main()