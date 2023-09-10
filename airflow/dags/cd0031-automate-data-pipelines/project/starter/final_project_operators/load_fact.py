from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    songplays_create_table = """
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id VARCHAR,
            start_time TIMESTAMP,
            user_id INTEGER,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INTEGER,
            location VARCHAR,
            user_agent VARCHAR
        )
        """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_data_query="",
                 table="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_data_query = load_data_query
        self.table = table
        self.truncate_table = truncate_table


    def execute(self, context):

        self.log.info('Connecting to Redshift')
        self.log.info(f'Redshift credentials: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Attempting to create {self.table} fact table')
        redshift.run(LoadFactOperator.songplays_create_table)

        if self.truncate_table:
            self.log.info(f'Attempting to truncate {self.table} fact table')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'Starting {self.table} load pipeline')
        redshift.run(self.load_data_query)
        self.log.info(f'{self.table} load pipeline complete')
