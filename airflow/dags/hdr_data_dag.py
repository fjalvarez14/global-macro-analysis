from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging
import os

sys.path.insert(0, '/opt/airflow/scripts')

from fetch_hdr_api import run_hdr_ingestion

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': [os.getenv('SMTP_USER')],
    'retries': 2,
    'retry_delay': timedelta(minutes=1),

}

with DAG(
    'fetch_hdr_data',
    default_args=default_args,
    description='Fetch UNDP HDR indicators (23 indicators) and load to DuckDB',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['undp_hdr', 'data_ingestion', 'human_development'],
) as dag:
    
    fetch_hdr_task = PythonOperator(
        task_id='fetch_hdr_indicators',
        python_callable=run_hdr_ingestion,
        op_kwargs={},  # Can pass custom start_year/end_year if needed
        doc_md="""
        ## UNDP HDR Data Ingestion
        
        Fetches 23 human development indicators from UNDP HDR API from year 2000 (default).
        Requires HDR_API_KEY environment variable.
        Data is validated with Pydantic and loaded to DuckDB raw schema.
        """
    )