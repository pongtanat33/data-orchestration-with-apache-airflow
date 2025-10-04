# templating.py
import datetime
import logging

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


# Create timezone-aware datetime
#local_tz = pendulum.timezone("Asia/Bangkok")



def show_date(date):
    logging.info(f"Date: {date}")


with DAG(
    dag_id="templating",
    start_date=datetime.datetime(2025, 10, 1),
    schedule="@daily"
):
    task_1 = BashOperator(
        task_id="task_1",
        bash_command="echo {{ data_interval_start }}",
    )

    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=show_date,
        op_kwargs={"date": "{{ data_interval_end }}"},
    )

# [task_1 ,task_2]

    task_1 >> task_2


#show_date(date="2025-01-01")