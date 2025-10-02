import datetime
from zoneinfo import ZoneInfo
from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pendulum import timezone

local_tz = timezone("Asia/Bangkok")

def _hello():
    print("hello world")


dag = DAG(
    "everysunday",
    schedule="* * * * 7",
    start_date=local_tz.datetime(2025, 10, 2, 0, 0),
    timezone=local_tz,
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
