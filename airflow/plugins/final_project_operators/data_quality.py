from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_pk_check = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables_pk_check = tables_pk_check


    def execute(self, context):
        self.log.info('Connecting to Redshift')
        self.log.info(f'Redshift credentials: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Verifying dimension tables non-empty')
        for table_name in self.tables_pk_check.keys():
            rec_count = redshift.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(rec_count) < 1 or len(rec_count[0]) < 1:
                raise AirflowFailException(f'Data quality check failed: unable to query table {table_name}')
            if rec_count[0][0] < 1:
                raise AirflowFailException(f'Data quality check failed: no records found in a table {table_name}')
        
        self.log.info('Verifying no NULLs in dimensions primary keys')
        for table_name in self.tables_pk_check.keys():
            records = redshift.get_records(f"SELECT * FROM {table_name} WHERE {self.tables_pk_check[table_name]} IS NULL")
            if len(records) > 0:
                raise AirflowFailException(f'Data quality check failed: NULL values found in {table_name} dimension table, {self.tables_pk_check[table_name]} field')

        self.log.info('Data quality check sucess')
