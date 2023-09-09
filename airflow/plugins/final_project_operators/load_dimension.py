from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_data_query="",
                 table="",
                 create_table_query="",
                 *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_data_query = load_data_query
        self.table = table
        self.create_table_query = create_table_query

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        self.log.info(f'Redshift credentials: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Attempting to create dimension table {self.table}')
        redshift.run(self.create_table_query)

        self.log.info(f'Truncating dimension table {self.table}')
        redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'Loading data to dimension table {self.table}')
        redshift.run(self.load_data_query)

        self.log.info(f'Dimension table {self.table}: data load success')
