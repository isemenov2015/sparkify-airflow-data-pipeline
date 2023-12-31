#Instructions
#In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
#1 - Read through the DAG and identify points in the DAG that could be split apart
#2 - Split the DAG into multiple tasks
#3 - Add necessary dependency flows for the new tasks
#4 - Run the DAG

# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator


@dag (
    start_date=pendulum.now()
)
def demonstrating_refactoring():

#
# Done: Finish refactoring this function into the appropriate set of tasks,
#       instead of keeping this one large task.
#
    # @task()
    # def load_and_analyze(*args, **kwargs):
    #     redshift_hook = PostgresHook("redshift")

    #     # Find all trips where the rider was under 18
    #     redshift_hook.run("""
    #         BEGIN;
    #         DROP TABLE IF EXISTS younger_riders;
    #         CREATE TABLE younger_riders AS (
    #             SELECT * FROM trips WHERE birthyear > 2000
    #         );
    #         COMMIT;
    #     """)
    #     records = redshift_hook.get_records("""
    #         SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    #     """)
    #     if len(records) > 0 and len(records[0]) > 0:
    #         logging.info(f"Youngest rider was born in {records[0][0]}")


    #     # Find out how often each bike is ridden
    #     redshift_hook.run("""
    #         BEGIN;
    #         DROP TABLE IF EXISTS lifetime_rides;
    #         CREATE TABLE lifetime_rides AS (
    #             SELECT bikeid, COUNT(bikeid)
    #             FROM trips
    #             GROUP BY bikeid
    #         );
    #         COMMIT;
    #     """)

    #     # Count the number of stations by city
    #     redshift_hook.run("""
    #         BEGIN;
    #         DROP TABLE IF EXISTS city_station_counts;
    #         CREATE TABLE city_station_counts AS(
    #             SELECT city, COUNT(city)
    #             FROM stations
    #             GROUP BY city
    #         );
    #         COMMIT;
    #     """)

    @task
    def younger_riders_create_table():
        redshift_hook = PostgresHook("redshift")

        # Find all trips where the rider was under 18
        redshift_hook.run("""
            BEGIN;
            DROP TABLE IF EXISTS younger_riders;
            CREATE TABLE younger_riders AS (
                SELECT * FROM trips WHERE birthyear > 2000
            );
            COMMIT;
        """)

    @task
    def younger_riders_analize():
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records("""
            SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
        """)
        if len(records) > 0 and len(records[0]) > 0:
            logging.info(f"Youngest rider was born in {records[0][0]}")

    @task
    def lifetime_rides_create_table():
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run("""
            BEGIN;
            DROP TABLE IF EXISTS lifetime_rides;
            CREATE TABLE lifetime_rides AS (
                SELECT bikeid, COUNT(bikeid)
                FROM trips
                GROUP BY bikeid
            );
            COMMIT;
        """)

    @task
    def city_station_counts_create_table():
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run("""
            BEGIN;
            DROP TABLE IF EXISTS city_station_counts;
            CREATE TABLE city_station_counts AS(
                SELECT city, COUNT(city)
                FROM stations
                GROUP BY city
            );
            COMMIT;
        """)

    @task
    def create_oldest_task():
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run("""
            BEGIN;
            DROP TABLE IF EXISTS older_riders;
            CREATE TABLE older_riders AS (
                SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
            );
            COMMIT;
        """)

    @task()
    def log_oldest():
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records("""
            SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
        """)
        if len(records) > 0 and len(records[0]) > 0:
            logging.info(f"Oldest rider was born in {records[0][0]}")

##    load_and_analyze = load_and_analyze()
    younger_riders_create_table = younger_riders_create_table()
    younger_riders_analize = younger_riders_analize()
    lifetime_rides_create_table = lifetime_rides_create_table()
    city_station_counts_create_table = city_station_counts_create_table()

    create_oldest_task = create_oldest_task()
    log_oldest_task = log_oldest()

    younger_riders_create_table >> younger_riders_analize
    create_oldest_task >> log_oldest_task

demonstrating_refactoring_dag = demonstrating_refactoring()
