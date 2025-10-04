import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import DAG
from airflow.models import Connection



def _extract_api():
    print("Extract Api")

def _extract_postgres():
    print("Extract Postgres")

def _extract_google_analytics():
    print("Extract Google Analytics")

def _transfrom_and_clean_data():
    print("_transfrom_and_clean_data")

def _load_data_warhouse():
    print("Load Data Warehouse")

def _generate_report():
    print("Generate Data")


with DAG(
    dag_id="dashboard",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    
    start= EmptyOperator(task_id="start")

    extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=_extract_api
    )

    extract_postgress = PythonOperator(
        task_id = "extract_postgres",
        python_callable = _extract_postgres
    )

    extract_google_analytics = PythonOperator(
        task_id = "extract_google_analytics",
        python_callable = _extract_google_analytics
    )

    transform_clean_data = PythonOperator(
        task_id = "transform_clean_data",
        python_callable = _transfrom_and_clean_data
    )

    load_to_data_warehouse = PythonOperator(
        task_id = "load_data_warhouse",
         python_callable = _load_data_warhouse
    )

    generate_report = PythonOperator(
        task_id = "generate_report",
         python_callable = _generate_report
    )


    # forecast = HttpOperator(
    #     task_id="get_data",
    #     http_conn_id='http_default',
    #     endpoint="https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m",
    #     method='GET',
    #     headers={'Content-Type': 'application/json'},
    #     log_response=True
    # )


    end = EmptyOperator(task_id="end")


    start >> [extract_api,extract_postgress,extract_google_analytics]  >> transform_clean_data >> load_to_data_warehouse  >> generate_report >> end
