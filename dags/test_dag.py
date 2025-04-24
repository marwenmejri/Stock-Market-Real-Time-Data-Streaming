from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id="print_hello",
        bash_command="echo Hello, Airflow!"
    )

    task2 = BashOperator(
        task_id="print_hello2",
        bash_command="echo Hello, Airflow!2"
    )

    task1 >> task2