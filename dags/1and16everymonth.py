from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def _hello():
    print("hello world")

with DAG(
    "day1and16",
    start_date= timezone.datetime(2025,10,2),
    schedule = "30 17 1,16 * *"
):
    hello = PythonOperator(
        task_id="hello",
        python_callable=_hello
    )

    world = BashOperator(
        task_id="world",
        bash_command="echo world"
    )

    hello >> world
