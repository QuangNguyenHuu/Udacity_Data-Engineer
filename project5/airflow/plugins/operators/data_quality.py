from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_connection_id=None, tables=[], data_quality_checks={}, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift = PostgresHook(postgres_conn_id=redshift_connection_id)
        self.tables = tables
        self.quality_checks = data_quality_checks

    def execute(self, context):
        self.log.info('START DataQualityOperator')
        result_checks = []
        for qc in self.quality_checks:
            result_check = {'table': None, 'query': None, 'query_result': None, 'check_result': None}
            self.log.info('START DATA QUALITY CHECK NULL')
            result_check['table'] = qc['table']
            for query in qc['check_queries']:
                results = self.redshift.get_records(query)
                result_check['query'] = query
                result_check['query_result'] = results[0]
                expected_result = qc['expected_result']
                actual_result = results[0][0]
                if expected_result is not None:
                    if actual_result != expected_result:
                        self.log.error('DATA QUALITY CHECK FAILED. Query: {0}'.format(query))
                        self.log.info('Expected result: [{0}] - Actual result: [{1}]'.format(expected_result, actual_result))
                        result_check['check_result'] = False
                    else:
                        self.log.info('DATA QUALITY CHECK PASSED. Query: {0}'.format(query))
                        result_check['check_result'] = True
                elif len(results[0]) < 1 or len(results) < 1 or results[0][0] < 1:
                    self.log.warning('SQL query [{}] has NO data'.format(query))
                    result_check['check_result'] = True
            result_checks.append(result_check)
        self.log.info("Checks' result: [{0}]".format(result_checks))
        self.log.info('END DataQualityOperator')
