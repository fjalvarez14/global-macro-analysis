from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': [os.getenv('SMTP_USER')],
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'dbt_soda_transform',
    default_args=default_args,
    description='Run dbt transformations and Soda data quality checks',
    schedule=None,  # Manual trigger after ingestion DAGs complete
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'data_quality', 'soda'],
) as dag:
    
    dbt_clean = BashOperator(
        task_id='dbt_clean',
        bash_command='cd /opt/airflow/dbt && dbt clean --profiles-dir .',
        doc_md="""
        ## Clean dbt Cache
        Removes compiled artifacts from target/ only:
        - Clears stale compiled tests
        - Ensures fresh compilation
        - Prevents orphaned test execution
        - Keeps dbt_packages/ intact
        """
    )
        
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps --profiles-dir .',
        doc_md="""
        ## Install dbt Packages
        Installs required dbt packages specified in packages.yml.
        This runs before model execution.
        """
    )
    
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
        doc_md="""
        ## Run dbt Models
        Executes all dbt models to transform raw data:
        - **Staging**: Clean and standardize raw data from 4 sources
        - **Marts**: Create dim_country and fact_macro_indicators
        """
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
        doc_md="""
        ## Run dbt Tests
        Executes built-in and custom dbt tests:
        - Uniqueness checks
        - Not-null checks
        - Referential integrity
        """
    )
    
    soda_ingest = BashOperator(
        task_id='soda_ingest_dbt_tests',
        bash_command='cd /opt/airflow/soda && soda ingest dbt -d global_macro_dbt -c configuration.yml --dbt-artifacts ../dbt/target',
        doc_md="""
        ## Soda Ingest dbt Tests
        Ingests dbt test results from manifest.json and run_results.json:
        - Reads all dbt test outcomes
        - Translates results to Soda format
        - Enables monitoring in Soda Cloud
        - No duplicate testing - just monitors dbt results
        """
    )
    
    # Define task dependencies
    dbt_clean >> dbt_deps >> dbt_run >> dbt_test >> soda_ingest
