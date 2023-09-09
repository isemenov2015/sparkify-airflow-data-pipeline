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
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_data_query = load_data_query

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        self.log.info(f'Redshift credentials: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Attempting to create songplays fact table')
        redshift.run(LoadFactOperator.songplays_create_table)

        self.log.info('Starting songplays fact load load pipeline')
        redshift.run(self.load_data_query)
        self.log.info('Songplays fact load pipeline complete')
