from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging
import os

sys.path.insert(0, '/opt/airflow/scripts')

from load_country_metadata import run_seed_ingestion

# Configure logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': [os.getenv('SMTP_USER')],
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'load_seed_data',
    default_args=default_args,
    description='Load country metadata seed data to DuckDB',
    schedule=None,  # Manual trigger only - seed data changes infrequently
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['seed_data', 'metadata', 'country_dimension'],
) as dag:
    
    load_seed_task = PythonOperator(
        task_id='load_country_metadata',
        python_callable=run_seed_ingestion,
        doc_md="""
        ## Country Metadata Seed Data
        
        Loads country dimension table with:
        - Country names, ISO codes
        - Geographic groupings (continent, region)
        - Economic classification (income group)
        - Membership in groups (EU, OECD, ASEAN, BRICS, G20, FCS)
        
        Run this DAG first before any data ingestion DAGs.
        """
    )