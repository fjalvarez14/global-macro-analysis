from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import logging
import os

sys.path.insert(0, '/opt/airflow/scripts')

from load_country_metadata import run_seed_ingestion
from fetch_wb_api import run_wb_ingestion
from fetch_hdr_api import run_hdr_ingestion
from fetch_imf_api import run_imf_ingestion

# Configure logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'global_macro_analysis',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': [os.getenv('SMTP_USER')],
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Complete data ingestion pipeline: seeds → [WB, HDR, IMF] → trigger transform',
    schedule=None,  
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['pipeline', 'data_ingestion', 'imf', 'world_bank', 'undp_hdr', 'seed_data'],
) as dag:
    
    load_seed_task = PythonOperator(
        task_id='load_country_metadata',
        python_callable=run_seed_ingestion,
        doc_md="""
        ### Load Country Metadata
        Loads country dimension table with:
        - Country names, ISO codes (ISO2, ISO3)
        - Geographic groupings (continent, region)
        - Economic classification (income group)
        - Membership flags (EU, OECD, ASEAN, BRICS, G20, FCS)
        **Target**: `raw.country_metadata` table
        """
    )
    
    fetch_wb_task = PythonOperator(
        task_id='fetch_wb_indicators',
        python_callable=run_wb_ingestion,
        doc_md="""
        ### Fetch World Bank Data
        Fetches 28 macroeconomic indicators from World Bank Open Data API.
        **Target**: `raw.wb_indicators` table
        """
    )
    
    fetch_hdr_task = PythonOperator(
        task_id='fetch_hdr_indicators',
        python_callable=run_hdr_ingestion,
        doc_md="""
        ### Fetch UNDP HDR Data
        Fetches 23 human development indicators from UNDP HDR API.
        Requires HDR_API_KEY environment variable.
        **Target**: `raw.hdr_indicators` table
        """
    )
    
    fetch_imf_task = PythonOperator(
        task_id='fetch_imf_indicators',
        python_callable=run_imf_ingestion,
        doc_md="""
        ### Fetch IMF Data
        Fetches IMF economic indicators from WEO and FDI datasets.
        **Target**: `raw.imf_weo_indicators` and `raw.imf_fdi_indicator` tables
        """
    )
    
    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_dbt_soda_transform',
        trigger_dag_id='dbt_soda_transform',
        wait_for_completion=False
    )
    
    # Define task dependencies
    load_seed_task >> [fetch_wb_task, fetch_hdr_task, fetch_imf_task] >> trigger_transform
