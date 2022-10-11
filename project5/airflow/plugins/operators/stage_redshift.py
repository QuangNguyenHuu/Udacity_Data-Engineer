from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 dag=None,
                 aws_credential=None,
                 bucket_name=None,
                 bucket_data=None,
                 redshift_connection_id=None,
                 table_name=None,
                 data_format=None,
                 data_type=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.dag = dag
        self.aws_credential = aws_credential
        self.bucket_name = bucket_name
        self.bucket_data = bucket_data
        self.redshift_connection_id = redshift_connection_id
        self.table_name = table_name
        self.data_format = data_format
        self.data_type = data_type

    def get_aws_credentials(self):
        aws_hook = AwsHook(self.aws_credential)
        return aws_hook.get_credentials()

    def execute(self, context):
        self.log.info('StageToRedshiftOperator has started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)

        self.log.info("Copying data from S3 to Redshift")
        data_files = self.bucket_data.format(**context)
        for _ in data_files[:5]:
            print(_)
        print('DATA FILE COUNT: ', len(data_files))
        file_path = self.__s3_resource_builder(data_files)

        credentials = self.get_aws_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key
        if self.data_type == 'json':
            query = SqlQueries.migrate_query_json_template.format(self.table_name, file_path, access_key, secret_key, self.data_format)
            redshift.run(query)
        elif self.data_type == "csv":
            default_delimiter = ','
            query = SqlQueries.migrate_query_csv_template.format(self.table_name, file_path, access_key, secret_key, self.data_format, 1, default_delimiter)
            redshift.run(query)
        else:
            self.log.info('Nothing executed. Data type {0} is NOT support.'.format(self.data_type))

    def __s3_resource_builder(self, relative_path):
        return "s3://{0}/{1}".format(self.bucket_name, relative_path)
        