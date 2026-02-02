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
    description='Run dbt transformations with Soda data quality checks before and after',
    schedule=None,  
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'data_quality', 'soda'],
) as dag:
    
    soda_check_staging = BashOperator(
        task_id='soda_check_staging',
        bash_command='cd /opt/airflow/soda && soda scan -d global_macro_warehouse -c configuration_raw.yml checks_raw.yml',
        doc_md="""
        ## Soda Pre-Transformation Checks
        Validates staging tables BEFORE dbt runs:
        - Row counts > 0
        - No duplicate country codes
        - No missing required fields
        - Valid year ranges (2000-2026)
        - Ensures clean input data
        """
    )
    
    dbt_clean = BashOperator(
        task_id='dbt_clean',
        bash_command='cd /opt/airflow/dbt && dbt clean --profiles-dir .',
        doc_md="""
        ## Clean dbt Cache
        Removes compiled artifacts from target/ only:
        - Clears stale compiled tests
        - Ensures fresh compilation
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
        - **Marts**: Create dim_country, fact_macro_indicators, and wide format tables
        """
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
        doc_md="""
        ## Run dbt Tests
        Executes dbt built-in tests:
        - Uniqueness checks (unique, unique_combination_of_columns)
        - Not-null checks
        - Referential integrity (relationships)
        - Accepted values
        """
    )
    
    soda_check_marts = BashOperator(
        task_id='soda_check_marts',
        bash_command='cd /opt/airflow/soda && soda scan -d global_macro_warehouse -c configuration_marts.yml checks_marts.yml',
        doc_md="""
        ## Soda Post-Transformation Checks
        Validates marts tables AFTER dbt runs:
        - All marts have data (row_count > 0)
        - No duplicate primary keys
        - No missing required fields
        - Schema validation
        - Ensures quality output for dashboards
        """
    )
    
    soda_check_staging >> dbt_clean >> dbt_deps >> dbt_run >> dbt_test >> soda_check_marts
