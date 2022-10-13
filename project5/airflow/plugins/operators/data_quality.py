from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_connection_id=None, tables=[], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift = PostgresHook(postgres_conn_id=redshift_connection_id)
        self.tables = tables

    def execute(self, context):
        self.log.info('START DataQualityOperator')
        result_checks = []
        check_null_queries = []
        check_count_queries = []
        if 'songplays' in self.tables:
            check_null_queries.append(SqlQueries.songplays_data_quality_check_null)
        if "users" in self.tables:
            check_null_queries.append(SqlQueries.users_data_quality_check_null)
        if "songs" in self.tables:
            check_null_queries.append(SqlQueries.songs_data_quality_check_null)
        if "artists" in self.tables:
            check_null_queries.append(SqlQueries.artists_data_quality_check_null)
        if "time" in self.tables:
            check_null_queries.append(SqlQueries.time_data_quality_check_null)
        for name in self.tables:
            check_count_queries.append(SqlQueries.count_query_template.format(name))

        dq_checks = [
            {'type': 'check_null', 'check_queries': check_null_queries, 'expected_result': 0},
            {'type': 'check_count', 'check_queries': check_count_queries, 'expected_result': None},
            {'type': 'other', 'check_queries': 'your queries', 'expected_result': 'your expected'}

        ]
        for check in dq_checks:
            result_check = {'null_check': None, 'count_check': None, 'query': None, 'result': None}
            if 'check_null' == check['type']:
                self.log.info('START DATA QUALITY CHECK NULL')
                for query in check['check_queries']:
                    results = self.redshift.get_records(query)
                    result_check['query'] = query
                    result_check['result'] = results[0]
                    if len(results) < 1 or len(results[0]) < 1:
                        self.log.error('DATA QUALITY CHECK NULL FAILED. Query [{0}] returns results'.format(query))
                        result_check['null_check'] = False
                    elif results[0][0] > check['expected_result']:
                        self.log.error('DATA QUALITY CHECK NULL FAILED. Query [{0]] contain [{1}] records'.format(query, results[0][0]))
                        result_check['null_check'] = False
                    if results[0][0] == check['expected_result']:
                        self.log.info('DATA QUALITY CHECK NULL PASSED. No NULL record')
                        result_check['null_check'] = True
                self.log.info('END DATA QUALITY CHECK NULL')
            if 'check_count' == check['type']:
                self.log.info('DATA QUALITY CHECK COUNT')
                for query in check['check_queries']:
                    results = self.redshift.get_records(query)
                    result_check['query'] = query
                    result_check['result'] = results[0]
                    if len(results[0]) < 1 or len(results) < 1 or results[0][0] < 1:
                        self.log.warning('SQL query [{}] has NO data'.format(query))
                        result_check['count_check'] = False
                    else:
                        self.log.info('DATA QUALITY CHECK COUNT PASSED. Query [{0}] had {1} records.'.format(query, results[0][0]))
                self.log.info('END DATA QUALITY CHECK COUNT')
            else:
                # define your test
                pass
            result_checks.append(result_check)
        self.log.info('Checks results: [{0}]'.format(result_checks))
        self.log.info('END DataQualityOperator')
