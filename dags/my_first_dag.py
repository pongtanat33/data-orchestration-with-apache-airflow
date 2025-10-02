from airflow.sdk import DAG
from airflow.utils import timeline
from airflow.providers.standard.empty import EmptyOperator

with DAT(
    "my_first_dag",
    start_date= timezone.daetime(2025,10,2),
    schedule = None
):
    
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    
     