from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    "my_first_dag",
    start_date= timezone.datetime(2025,10,2),
    schedule = None
    
):


    tasks = []
    for i in range(9):
       tasks.append(EmptyOperator(task_id=f"task_{i+1}"))
       print(f"task_{i+1}")

    tasks[0] >> tasks[1] >> tasks[2] >> tasks[3]>> tasks[8]  
    tasks[0] >> tasks[4] >> tasks[5] 
    tasks[6] >> tasks[7] 
    tasks[1] >> tasks[6] 
       
