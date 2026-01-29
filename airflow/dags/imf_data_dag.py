from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging
import os

sys.path.insert(0, '/opt/airflow/scripts')

from fetch_imf_api import run_imf_ingestion

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
    'fetch_imf_data',
    default_args=default_args,
    description='Fetch IMF WEO indicators (7 indicators) + FDI data and load to DuckDB',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['imf', 'data_ingestion', 'fiscal_indicators', 'fdi'],
) as dag:
    
    fetch_imf_task = PythonOperator(
        task_id='fetch_imf_indicators',
        python_callable=run_imf_ingestion,
        op_kwargs={},  # Can pass custom start_year/end_year if needed
        doc_md="""
        ## IMF Data Ingestion
        
        Fetches IMF economic indicators from the WEO and FDI datasets from year 2000 (default).
        Data is validated with Pydantic and loaded to DuckDB raw schema.
        """
    )