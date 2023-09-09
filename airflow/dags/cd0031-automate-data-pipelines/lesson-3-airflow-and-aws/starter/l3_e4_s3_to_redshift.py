import pendulum
import logging

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


# Use the PostgresHook to create a connection using the Redshift credentials from Airflow 
# Use the PostgresOperator to create the Trips table
# Use the PostgresOperator to run the LOCATION_TRAFFIC_SQL


from udacity.common import sql_statements

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift():


    @task
    def load_task():    
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        logging.info(vars(aws_connection))
# Done: create the redshift_hook variable by calling PostgresHook()
        redshift_hook = PostgresHook('redshift')
        redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(aws_connection.login, aws_connection.password))

# Done: create the create_table_task by calling PostgresOperator()
    drop_table_task = PostgresOperator(
            task_id="drop_table_trips",
            postgres_conn_id="redshift",
            sql=sql_statements.DROP_TRIPS_TABLE_SQL            
        )
# Done: create the create_table_task by calling PostgresOperator()
    create_table_task = PostgresOperator(
            task_id="create_table_trips",
            postgres_conn_id="redshift",
            sql=sql_statements.CREATE_TRIPS_TABLE_SQL            
        )

# Done: Drop station_traffic table if exists
    drop_location_traffic_task = PostgresOperator(
            task_id="location_traffic_drop",
            postgres_conn_id="redshift",
            sql=sql_statements.DROP_LOCATION_TRAFFIC_SQL       
        )
# Done: create the location_traffic_task by calling PostgresOperator()
    location_traffic_task = PostgresOperator(
            task_id="location_traffic",
            postgres_conn_id="redshift",
            sql=sql_statements.LOCATION_TRAFFIC_SQL       
        )

    load_data = load_task()

# Done: uncomment the dependency flow for these new tasks    
    drop_table_task >> create_table_task
    create_table_task >> load_data
    load_data >> drop_location_traffic_task
    drop_location_traffic_task >> location_traffic_task

s3_to_redshift_dag = load_data_to_redshift()
