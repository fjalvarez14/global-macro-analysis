from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging

sys.path.insert(0, '/opt/airflow/scripts')

from fetch_wb_api import run_wb_ingestion

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_wb_data',
    default_args=default_args,
    description='Fetch World Bank indicators (35 indicators) and load to DuckDB',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['world_bank', 'data_ingestion', 'macro_indicators'],
) as dag:
    
    fetch_wb_task = PythonOperator(
        task_id='fetch_wb_indicators',
        python_callable=run_wb_ingestion,
        op_kwargs={},  # Can pass custom start_year/end_year if needed
        doc_md="""
        ## World Bank Data Ingestion
        
        Fetches 35 macroeconomic indicators from World Bank API:
        - GDP metrics, inflation, trade, employment
        - Inequality, education, health spending
        - 26-year rolling window (default)
        
        Data is validated with Pydantic and loaded to DuckDB raw schema.
        """
    )