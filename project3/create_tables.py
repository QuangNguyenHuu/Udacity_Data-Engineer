import psycopg2
from sql_queries import *


def drop_tables(cur, conn):
    """
    Drops each table which has defined in `drop_table_queries` list.
    :param cur: cursor of connection to connected database.
    :param conn: connection of database
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('Query: [{}]. Table dropped. DONE!'.format(query))


def create_tables(cur, conn):
    """
     Creates each table which has defined in `create_table_queries` list.
    :param cur: cursor of connection to connected database.
    :param conn: connection of database
    :return:
    """
    for query in create_table_queries:
        print('Query: [{}]. Table created. DONE!'.format(query))
        cur.execute(query)
        conn.commit()


def main():
    rr = ResourceReader()
    config = rr.load_config()

    conn_string = "host={} dbname={} user={} password={} port={}".format(
        config["CLUSTER"]['HOST'], config['CLUSTER']['DB_NAME'], config['CLUSTER']['DB_USER'],
        config['CLUSTER']['DB_PASSWORD'], config['CLUSTER']['DB_PORT'])
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
