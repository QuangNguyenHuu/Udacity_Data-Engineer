import re

from cassandra.cluster import Cluster
from queries import *

s_session = None
s_cluster = None


def create_cluster():
    """
    Establishes connection of Cassandra cluster
    :return: @cluster,
    """
    # default listener port
    # cluster = Cluster(['127.0.0.1'], port=9042)
    global s_cluster
    s_cluster = Cluster(['127.0.0.1'])

    return s_cluster


def create_session(keyspace_name):
    """
    Establishes Cassandra session
    :param keyspace_name: name of keyspace
    :return: @session
    """
    global s_cluster
    global s_session
    if s_cluster:
        s_session = s_cluster.connect()
    else:
        s_session = create_cluster().connect()

    create_keyspace(s_session, keyspace_name)
    return s_session


def create_keyspace(session, name=None, command=None):
    """
    To create keyspace
    :param command: optional command of creating keyspace
    :param session: session of Casandra
    :param name: name of keyspace
    :return: None
    """
    __default_cmd = '''
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
       }
    '''
    if command:
        cmd = command
    else:
        cmd = re.sub(r"{KEYSPACE}", name, __default_cmd)

    try:
        session.execute(cmd)
        session.set_keyspace(name)
        print('Keyspace "{}" created'.format(name))
    except Exception as e:
        print('Keyspace creating error: ', e)


def stop_session(session):
    """
    To shutdown the session
    :return: None
    """
    if session:
        session.shutdown()
        print('Session shutdown completed')


def stop_cluster(session):
    """
    To shutdown the cluster
    :return: None
    """
    global s_cluster
    if session:
        session.shutdown()
        print('Cluster shutdown completed')


def execute_queries(session, queries):
    """
    To execute queries
    :param session: working session of Cassandra
    :param queries: list of query
    :return: None
    """
    # global s_session
    for query in queries:
        print(query)
        results = session.execute(query)
        for r in results:
            print(r)
        # if results.current_rows:
        #     print(result[0])
        # else:
        #     print("Empty")


def execute_query(session, query):
    """
    To execute queries
    :param session: working session of Cassandra
    :param query: query string
    :return: None
    """
    # global s_session
    print(query)
    return session.execute(query)


def drop_tables(session):
    """
    To drop tables which specify in the query @drop_table_queries
    :param session: working session of Cassandra
    :return: None
    """
    global s_session
    for query in drop_table_queries:
        print(query)
        session.execute(query)


def create_tables(session):
    """
    To init tables which specify in the query @create_table_queries
    :param session: working session of Cassandra
    :return: None
    """
    # global s_session
    for query in create_table_queries:
        print(query)
        session.execute(query)
        print('Following table created successful')


def main():
    keyspace_name = 'sparkify_test1'
    cluster = create_cluster()
    session = create_session(keyspace_name)
    drop_tables(session)
    create_tables(session)
    result = execute_query(session, select_song_query)
    if len(result.current_rows) > 0:
        print(result[0])
    else:
        print("Empty")
    execute_queries(session, find_song_queries)

    stop_session(session)
    stop_cluster(cluster)
    print('Close cassandra session/cluster connection')


if __name__ == "__main__":
    main()
