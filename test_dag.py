#test dag
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def _print_hello():
    """A simple Python function that prints 'Hello'."""
    print("Hello")

def _print_world():
    """Another simple Python function that prints 'World'."""
    print("World")

with DAG(
    dag_id="simple_test_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "simple"],
) as dag:
    
    print_hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=_print_hello,
    )

    print_world_task = PythonOperator(
        task_id="print_world",
        python_callable=_print_world,
    )

    # Define the task dependency: print_hello_task runs before print_world_task
    print_hello_task >> print_world_task
