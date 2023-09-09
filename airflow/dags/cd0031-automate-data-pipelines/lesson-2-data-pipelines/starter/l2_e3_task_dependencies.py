import pendulum
import logging

from airflow.decorators import dag, task

@dag(
    description='Exercise 3, tasks order definition. My code',
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
def task_dependencies():

    @task()
    def hello_world():
        logging.info("Hello World")

    @task()
    def addition(first,second):
        logging.info(f"{first} + {second} = {first+second}")
        return first+second

    @task()
    def subtraction(first,second):
        logging.info(f"{first -second} = {first-second}")
        return first-second

    @task()
    def division(first,second):
        logging.info(f"{first} / {second} = {int(first/second)}")   
        return int(first/second)     

    @task()
    def goodbye_world():
        logging.info("Goodbye, World!")


# Done: call the hello world task function
# Done: call the addition function with some constants (numbers)
# Done: call the subtraction function with some constants (numbers)
# Done: call the division function with some constants (numbers)
# Done: create the dependency graph for the first three tasks
# Done: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

#  Done: assign the result of the addition function to a variable
#  Done: assign the result of the subtraction function to a variable
#  Done: pass the result of the addition function, and the subtraction functions to the division function
# Done: create the dependency graph for the last three tasks

    hello_world_task = hello_world()
    addition_task = addition(2000, 5300)
    subtraction_task = subtraction(400, 300)
    division_task = division(addition_task, subtraction_task)
    goodbye_world_task = goodbye_world()

    hello_world_task >> addition_task
    hello_world_task >> subtraction_task

    addition_task >> division_task
    subtraction_task >> division_task

    division_task >> goodbye_world_task

task_dependencies_dag=task_dependencies()