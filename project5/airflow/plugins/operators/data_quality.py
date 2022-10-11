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
        for name in self.tables:
            self.log.info('Count tables data')
            records = self.redshift.get_records(SqlQueries.count_query_template.format(name))
            if len(records[0]) < 1 or len(records) < 1 or records[0][0] < 1:
                self.log.warning('Table [{}] has NO data'.format(name))
            else:
                records = self.redshift.get_records(SqlQueries.select_query_template.format(name))
                self.log.info('Table [{}] data:'.format(name))
                for record in records:
                    self.log.info('{}'.format(name, record))
        self.log.info('END DataQualityOperator')
