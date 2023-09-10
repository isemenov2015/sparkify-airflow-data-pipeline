from datetime import datetime, timedelta
import pendulum
import os
from airflow.models import Variable
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements as fp_sql


default_args = {
    'owner': 'udacity',
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
    'email_on_retry': False,
    'depends_on_past': True,
    'start_date': pendulum.now(),
    'catchup': False,
    }

# dag = DAG('final_project_legacy',
#           default_args=default_args,
#           description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
#         )

@dag(
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1,
)
def sparkify_data_pipeline():

    start_operator = DummyOperator(
        task_id='Begin_execution',  
    )

    sql_queries = fp_sql.SqlQueries()

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        table='staging_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=Variable.get('s3_bucket'),
        s3_key='log-data',
        create_table_query="""
                CREATE TABLE IF NOT EXISTS staging_events (
                    artist TEXT,
                    auth VARCHAR,
                    firstName VARCHAR,
                    gender VARCHAR,
                    itemInSession INTEGER,
                    lastName VARCHAR,
                    length NUMERIC,
                    level VARCHAR,
                    location VARCHAR,
                    method VARCHAR,
                    page VARCHAR,
                    registration NUMERIC,
                    sessionId INTEGER,
                    song VARCHAR,
                    status INTEGER,
                    ts NUMERIC,
                    userAgent VARCHAR,
                    userId INTEGER
                )
                """,
        json_format='s3://udacity-aws-sparkify-project/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        table='staging_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=Variable.get('s3_bucket'),
        s3_key='song-data',
        create_table_query="""
                CREATE TABLE IF NOT EXISTS staging_songs (
                    num_songs INTEGER,
                    artist_id TEXT NOT NULL,
                    artist_latitude NUMERIC,
                    artist_longitude NUMERIC,
                    artist_location VARCHAR(4096),
                    artist_name VARCHAR(4096),
                    song_id TEXT NOT NULL,
                    title VARCHAR(4096),
                    duration NUMERIC,
                    year INTEGER
                )
                """,
        json_format='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        load_data_query=f"""
            INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
            {sql_queries.songplay_table_insert}
        """,
        table='songplays',
        truncate_table=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        load_data_query=f"""
            INSERT INTO users (userid, firstname, lastname, gender, level) 
            {sql_queries.user_table_insert}
        """,
        table='users',
        create_table_query="""
            CREATE TABLE IF NOT EXISTS users (
                userid INTEGER,
                firstname VARCHAR,
                lastname VARCHAR,
                gender VARCHAR,
                level VARCHAR
            )
        """
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        load_data_query=f"""
            INSERT INTO songs (song_id, title, artist_id, year, duration) 
            {sql_queries.song_table_insert}
        """,
        table='songs',
        create_table_query="""
            CREATE TABLE IF NOT EXISTS songs (
                song_id VARCHAR,
                title VARCHAR(4096),
                artist_id VARCHAR,
                year INTEGER,
                duration NUMERIC
            )
        """
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        load_data_query=f"""
            INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) 
            {sql_queries.artist_table_insert}
        """,
        table='artists',
        create_table_query="""
            CREATE TABLE IF NOT EXISTS artists (
                artist_id VARCHAR,
                artist_name VARCHAR(4096),
                artist_location VARCHAR(4096),
                artist_latitude NUMERIC,
                artist_longitude NUMERIC
            )
        """
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        load_data_query=f"""
            INSERT INTO times (start_time, hour, day, week, month, year, dayofweek) 
            {sql_queries.time_table_insert}
        """,
        table='times',
        create_table_query="""
            CREATE TABLE IF NOT EXISTS times (
                start_time TIMESTAMP,
                hour INTEGER,
                day INTEGER,
                week INTEGER,
                month INTEGER,
                year INTEGER,
                dayofweek INTEGER
            )
        """
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables_pk_check={'songs': 'song_id', 'artists': 'artist_id', 'users': 'userid', 'times': 'start_time'},
    )

    finish_operator = DummyOperator(
        task_id='End_execution',  
    )

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> finish_operator


airflow_project = sparkify_data_pipeline()
