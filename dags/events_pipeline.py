from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
sys.path.append('/opt/airflow/scripts')

from fetch_events import fetch_events
from transform_events import transform_events

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'events_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for events data',
    schedule_interval=timedelta(days=1),
)

# Task 1: Fetch events
fetch_task = PythonOperator(
    task_id='fetch_events',
    python_callable=fetch_events,
    op_kwargs={
        'city': 'Austin',
        'api_key': os.getenv('SERPAPI_KEY')
    },
    dag=dag,
)

# Task 2: Transform events
transform_task = PythonOperator(
    task_id='transform_events',
    python_callable=transform_events,
    # Use the path from the fetch_events task
    op_kwargs={
        'input_path': "{{ task_instance.xcom_pull(task_ids='fetch_events') }}"
    },
    dag=dag,
)

# Set task dependencies
fetch_task >> transform_task