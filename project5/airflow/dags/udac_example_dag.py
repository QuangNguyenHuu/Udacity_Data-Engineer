from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Configuring the DAG
# In the DAG, add default parameters according to these guidelines
# -----------------------------------------------------------------
# The DAG does not have dependencies on past runs
# On failure, the task are retried 3 times
# Retries happen every 5 minutes
# Catchup is turned off
# Do not email on retry
# -----------------------------------------------------------------

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 10, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'depends_on_past': False
}


dag = DAG('MY_DAG_TEST', default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

# airflow 1.10.10
start_operator = DummyOperator(task_id='BEGIN_EXECUTION',  dag=dag)

aws_credential = 'aws_credential'
redshift_conn = 'redshift_conn'
bucket_name = 'udacity-dend'
bucket_data = 'log_data/2018/11/'
json_extension = 'json'

stage_events_to_redshift = StageToRedshiftOperator(task_id='STAGE_EVENTS', dag=dag,
                                                   aws_credential=aws_credential,
                                                   bucket_name=bucket_name,
                                                   bucket_data=bucket_data,
                                                   redshift_connection_id=redshift_conn,
                                                   table_name='stage_events',
                                                   data_format='s3://udacity-dend/log_json_path.json',
                                                   data_type=json_extension, start_date=datetime(2022, 10, 10))

stage_songs_to_redshift = StageToRedshiftOperator(task_id='STAGE_SONGS', dag=dag,
                                                  aws_credential=redshift_conn,
                                                  aws_bucket_name=bucket_name,
                                                  redshift_connection_id=redshift_conn,
                                                  bucket_data=bucket_data,
                                                  table_name='staging_songs',
                                                  data_format='auto',
                                                  data_type=json_extension, start_date=datetime(2022, 10, 10))

load_songplays_table = LoadFactOperator(task_id='LOAD_SONGPLAYS_FACT_TABLE', dag=dag,
                                        redshift_connection_id=redshift_conn,
                                        table_name='songplays',
                                        statment=SqlQueries.songplay_table_insert, start_date=datetime(2022, 10, 10))

load_user_dimension_table = LoadDimensionOperator(task_id='LOAD_USER_TABLE', dag=dag,
                                                  redshift_connection_id=redshift_conn,
                                                  table_name='users',
                                                  statment=SqlQueries.user_table_insert, start_date=datetime(2022, 10, 10))

load_song_dimension_table = LoadDimensionOperator(task_id='LOAD_SONG_TABLE', dag=dag,
                                                  redshift_connection_id=redshift_conn,
                                                  table_name='songs',
                                                  statment=SqlQueries.song_table_insert, start_date=datetime(2022, 10, 10))

load_artist_dimension_table = LoadDimensionOperator(task_id='LOAD_ARTIST_TABLE', dag=dag,
                                                    redshift_connection_id=redshift_conn,
                                                    table_name='artists',
                                                    statment=SqlQueries.artist_table_insert, start_date=datetime(2022, 10, 10))

load_time_dimension_table = LoadDimensionOperator(task_id='LOAD_TIME_TABLE', dag=dag,
                                                  redshift_connection_id=redshift_conn,
                                                  table_name='time',
                                                  statment=SqlQueries.time_table_insert)

run_quality_checks = DataQualityOperator(task_id='DATA_QUALITY_CHECKS', dag=dag,
                                         redshift_connection_id=redshift_conn,
                                         tables=['songplays', 'users', 'songs', 'artists', 'time'],
                                         start_date=datetime(2022, 10, 10))

end_operator = DummyOperator(task_id='STOP_EXECUTION',  dag=dag)


# DEPENDENCIES & TASK ORDER

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
