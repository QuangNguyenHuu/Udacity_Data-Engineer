import os
from resource_reader import *
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, to_timestamp, col, year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
from sql_queries import *


def create_spark_session():
    """
    Creates session of spark
    :return: spark session
    """
    session = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1").getOrCreate()
    print('Spark session created')
    return session


def create_spark_session_s3(config):
    """
    Initiates AWS spark session
    :param config: AWS properties
    :return:
    """
    conf = SparkConf()
    # conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1')
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set('spark.hadoop.fs.s3a.access.key', config['AWS']['ACCESS_KEY_ID'])
    conf.set('spark.hadoop.fs.s3a.secret.key', config['AWS']['SECRET_ACCESS_KEY'])
    session = SparkSession.builder.config(conf=conf).getOrCreate()
    print('S3 spark session created')
    return session


def init_spark_data(spark, path):
    """
    Establish spark data
    :param spark: session of spark
    :param path: path of data
    :return: spark data
    """
    spark_data = spark.read.json(path)
    print('Spark data has established')
    return spark_data


def create_table(session, data, query, template_view=None):
    """
    Creates table
    :param session: session of spart
    :param data: data need to process
    :param query: query template
    :param template_view: to create view of processing table
    :return: processed table
    """
    if template_view:
        query = query.format(template_view)
        data.createOrReplaceTempView(template_view)
        return session.sql(query)
    data.createOrReplaceTempView(default_view_template)
    return session.sql(query)


def create_nextsong_table(session, song_data, log_data):
    """
    Creates "NextSong" table by inner join "song data" & "log data"
    :param session: session of spark
    :param song_data: song data to process
    :param log_data: log data to process
    :return: "NextSong" table
    """
    song_data.createOrReplaceTempView('song_data_view')
    log_data.createOrReplaceTempView('nextsong_data_view')
    return session.sql(songplays_table_query_template)


def save_as_parquet_format(table, path, mode=None, order_by=None):
    """
    Writes table data as parquet files format
    :param table: table data which need to write parquet files
    :param path: path of saving parquet files
    :param mode: mode of writing (append/overwrite)
    :param order_by: which data processing order
    :return: None
    """
    if not mode:
        mode = 'overwrite'
    if not order_by:
        table.write.mode(mode).parquet(str(path))
        print('[PARQUET] files saved success')
        return
    table.write.mode(mode).partitionBy(order_by).parquet(str(path))
    print('[PARQUET] files saved success')


def read_parquet_format(session, path):
    """
    Reads parquet files format
    :param session: session of spark
    :param path: path of parquet directory
    """
    return session.read.parquet(path)


def main():
    rr = ResourceReader()
    config = rr.config_reader('dwh.yml')
    ACCESS_KEY_ID = config['AWS']['ACCESS_KEY_ID']
    SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY']
    REGION = config['AWS']['REGION']

    os.environ["AWS_ACCESS_KEY_ID"] = ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = SECRET_ACCESS_KEY

    rr.setup_aws_services(ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION)
    # spark_session = create_spark_session()
    # songs_data = init_spark_data(spark_session, config['LOCAL']['SONG_DATA'])
    # songs_data.printSchema()
    #
    # logs_data = init_spark_data(spark_session, config['LOCAL']['LOG_DATA'])
    # logs_data_nextsong = logs_data.filter(logs_data.page == 'NextSong')
    # # adds songplay_id clumn, time_stamp column by extract from "ts" data
    # logs_data_nextsong = logs_data_nextsong\
    #     .withColumn('time_stamp', to_timestamp(logs_data_nextsong.ts / 1000))\
    #     .withColumn('songplay_id', monotonically_increasing_id())
    # logs_data_nextsong.printSchema()
    #
    # songs_tbl = create_table(spark_session, songs_data, songs_table_query_template)
    # song_path = rr.resource_path_builder('output-data', 'parquet', 'song-data')
    # save_as_parquet_format(songs_tbl, song_path, order_by=['artist_id', 'year'])
    #
    # artists_tbl = create_table(spark_session, songs_data, artists_table_query_template)
    # artists_tbl.printSchema()
    # artist_path = rr.resource_path_builder('output-data', 'parquet', 'artist-data')
    # save_as_parquet_format(artists_tbl, artist_path, order_by='artist_id')
    #
    # users_tbl = create_table(spark_session, logs_data_nextsong, users_table_query_template)
    # users_tbl.printSchema()
    # user_path = rr.resource_path_builder('output-data', 'parquet', 'artist-data')
    # save_as_parquet_format(users_tbl, user_path, order_by=['first_name', 'last_name'])
    #
    # time_tbl = create_table(spark_session, logs_data_nextsong, time_table_query_template)
    # time_tbl.printSchema()
    # time_path = rr.resource_path_builder('output-data', 'parquet', 'time-data')
    # save_as_parquet_format(time_tbl, time_path, order_by=['year', 'month'])
    #
    # nextsong_tbl = create_nextsong_table(spark_session, songs_data, logs_data_nextsong)
    # nextsong_path = rr.resource_path_builder('output-data', 'parquet', 'nextsong-data')
    # save_as_parquet_format(nextsong_tbl, nextsong_path, order_by='songplay_id')
    #
    spark_s23 = create_spark_session_s3(config)
    bucket = rr.s3_resource.Bucket(config['S3']['BUCKET_NAME'])
    song_data_files = [f.key for f in bucket.objects.filter(Prefix='song_data')]
    log_data_files = [f.key for f in bucket.objects.filter(Prefix='log_data')]
    for _ in log_data_files[:5]:
        print(_)
        # logs_data = init_spark_data(spark_s23, _)

    # s3://myeyesbucket.test/song_data/
    path = 's3a://myeyesbucket.test/song_data/A/A/A/TRAAAAW128F429D538.json'
    ss = spark_s23.read.json(path)
    print(ss)
    songs_data = init_spark_data(spark_s23, path)
    
    # for _ in song_data_files[:1]:
    #     print(_)
    #     path = 's3a://myeyesbucket/song_data/A/A/A/TRAAAAW128F429D538.json'
    #     songs_data = init_spark_data(spark_s23, path)
    # print('SONG DATA COUNT: ', len(song_data_files))
    #
    # song_data_path = config['S3']['SONG_DATA']
    # print(song_data_path)
    # song_data_s3 = rr.s3_resource.Bucket(song_data_path)
    # bucket = rr.s3_resource.Bucket(config['S3']['BUCKET_NAME'])
    # count_sd = 0
    # for obj in song_data_s3.objects.filter(Prefix="data"):
    #     # init_spark_data(spark, path)
    #     count_sd += 1
    #     print(obj)
    # print(count_sd)


if __name__ == "__main__":
    main()
