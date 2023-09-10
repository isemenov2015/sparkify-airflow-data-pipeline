from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
        TIMEFORMAT AS 'epochmillisecs'
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_region='us-east-1',
                 create_table_query="",
                 json_format='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.create_table_query = create_table_query
        self.json_format = json_format


    def execute(self, context):
        self.log.info('Reading AWS credentials')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info('Connecting to Redshift')
        self.log.info(f'Redshift credentials: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Starting data load pipeline for table {self.table}')

        self.log.info(f"Attempting to create Redshift table {self.table}")
        redshift.run(self.create_table_query)

        self.log.info(f"Truncating Redshift table {self.table}")
        redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_region,
            self.json_format,
        )
        redshift.run(formatted_sql)

        self.log.info(f'Data load pipeline for table {self.table} complete')
