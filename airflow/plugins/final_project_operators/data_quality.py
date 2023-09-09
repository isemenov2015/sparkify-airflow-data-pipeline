from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id


    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        self.log.info('Connecting to Redshift')
        self.log.info(f'Redshift credentials: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Verifying dimension tables non-empty')
        for table_name in ('songs', 'users', 'times', 'artists'):
            records = redshift.get_records(f"SELECT * FROM {table_name}")
            if len(records) == 0:
                raise AirflowFailException(f'Data quality check failed: empty table {table_name}')
        
        self.log.info('Verifying no NULLs in dimensions primary keys')
        records = redshift.get_records("SELECT * FROM songs WHERE song_id IS NULL")
        if len(records) > 0:
            raise AirflowFailException('Data quality check failed: NULL values found in users dimension table, song_id field')

        records = redshift.get_records("SELECT * FROM artists WHERE artist_id IS NULL")
        if len(records) > 0:
            raise AirflowFailException('Data quality check failed: NULL values found in artists dimension table, artist_id field')

        records = redshift.get_records("SELECT * FROM users WHERE userid IS NULL")
        if len(records) > 0:
            raise AirflowFailException('Data quality check failed: NULL values found in users dimension table, userid field')

        records = redshift.get_records("SELECT * FROM times WHERE start_time IS NULL")
        if len(records) > 0:
            raise AirflowFailException('Data quality check failed: NULL values found in times dimension table, start_time field')

        self.log.info('Data quality check sucess')
