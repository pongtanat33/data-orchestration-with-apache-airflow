import datetime
import logging
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG,task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

# customers_df = pd.read_csv(...)
# # Format the datetime from "12 May 1990" to "1990-05-12"
# df["birthdate"] = pd.to_datetime(df["birthdate"], dayfirst=True)

# Idea
# s3_hook = S3Hook(aws_conn_id="my_aws_connection")
# s3_hook.load_file(
#     filename=...,
#     key=...,
#     bucket_name="pea-watt",
#     replace=True
# )

def _tranfrom_to_s3(file_name,key):
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")
    s3_hook.load_file(
        filename=file_name,              
        key=key,         
        bucket_name="pea-watt",           
        replace=True                         
    )


@task(task_id="extract_customer")
def _extract_customer():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")

    df = hook.get_pandas_df("SELECT * FROM customers;")

    logging.info(f"Columns: {df.columns.tolist()}")
    logging.info(df)

    df["birthdate"] = pd.to_datetime(df["birthdate"], dayfirst=True)
    logging.info(df["birthdate"])
    #df.to_parquet('customers.parquet', index=False)

    # Save to Parquet
    output_file = '/tmp/customers.parquet'
    df.to_parquet(output_file, 
                      index=False, 
                      compression='snappy')

    print(f"Saved {len(df)} records to {output_file}")
    logging.info(f"Saved to {output_file}")
    _tranfrom_to_s3(output_file,'pongtanat/2025-10-03/customers.parquet')
    return output_file

@task(task_id="extract_order")
def _extract_order():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    df = hook.get_pandas_df("SELECT * FROM orders;")
    output_file = '/tmp/order.parquet'
    df.to_parquet(output_file, 
                      index=False, 
                      compression='snappy')
    
    output_file = '/tmp/order.parquet'
    print(f"Saved {len(df)} records to {output_file}")
    logging.info(f"Saved to {output_file}")

    _tranfrom_to_s3(output_file,'pongtanat/2025-10-03/orders.parquet')
    return output_file


with DAG(
    dag_id="tranform",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    
    start= EmptyOperator(task_id="start")

    extract_to_customer=_extract_customer()
    extract_to_orders=_extract_order()

    end = EmptyOperator(task_id="end")

    start >> [extract_to_customer,extract_to_orders] >> end
    

