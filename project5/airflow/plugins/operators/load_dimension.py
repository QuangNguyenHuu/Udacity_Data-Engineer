from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_connection_id=None, table_name=None, sql_query=None, append=True, *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id = redshift_connection_id
        self.table_name = table_name
        self.sql_query = sql_query
        self.mode = append
        self.redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)

    def execute(self, context):
        self.log.info('LoadDimensionOperator starts...')
        query = None
        if not self.sql_query:
            self.log.warning('Query is BLANK')
            return

        if not self.mode:
            self.log.info('Delete table [{0}] data'.format(self.table_name))
            query = SqlQueries.delete_query_template.format(self.table_name)
            self.execute_then_commit(query)
            self.log.info('Data deleted success'.format(self.table_name))

            query = SqlQueries.insert_query_template.format(self.table_name, self.sql_query)
        else:
            self.log.info('Insert data into [{0}] table'.format(self.table_name))
            query = SqlQueries.insert_query_template.format(self.table_name, self.sql_query)

        self.execute_then_commit(query)
        self.log.info('Table [{0}] data loaded success'.format(self.table_name))

    def execute_then_commit(self, query):
        self.redshift.run(query)
        self.redshift.run(SqlQueries.sql_commit_query)
