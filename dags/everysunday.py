import datetime
from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def _hello():
    print("hello world")


dag = DAG(
    "everysunday",
    schedule="* * * * 7",
    start_date= timezone.datetime(2025,10,2),
)

hello = PythonOperator(
    task_id="hello",
    python_callable=_hello,
    dag=dag
)

world = BashOperator(
    task_id="world",
    bash_command="echo world",
    dag=dag
)

hello >> world
