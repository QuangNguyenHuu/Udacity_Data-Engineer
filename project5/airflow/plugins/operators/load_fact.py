from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, redshift_connection_id=None, table_name=None, sql_query=None, *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.sql_query = sql_query
        self.redshift = PostgresHook(postgres_conn_id=redshift_connection_id)

    def execute(self, context):
        self.log.info('LoadFactOperator starts...')
        query = SqlQueries.insert_query_template.format(self.table_name, self.sql_query)

        self.log.info('Insert data into fact table [{}]'.format(self.table_name))
        self.execute_then_commit(query)

    def execute_then_commit(self, query):
        self.redshift.run(query)
        self.redshift.run(SqlQueries.sql_commit_query)
        