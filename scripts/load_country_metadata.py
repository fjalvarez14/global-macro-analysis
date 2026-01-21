import pandas as pd
import duckdb
from pathlib import Path
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Path configuration - Docker-aware
def get_base_path():
    """Returns base path depending on environment (Docker or local)"""
    if os.path.exists('/opt/airflow'):
        return Path('/opt/airflow')
    return Path(__file__).parent.parent

def run_seed_ingestion():
    """Load country metadata CSV to DuckDB raw schema
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get paths
        base_path = get_base_path()
        csv_path = base_path / 'data' / 'seeds' / 'country_continent_group.csv'
        db_path = base_path / 'data' / 'warehouse.duckdb'
        backup_path = base_path / 'data_backup' / 'country_continent_group.csv'
        
        logger.info("Loading country metadata...")
        
        # Read CSV
        df = pd.read_csv(csv_path)
        logger.info(f"Read {len(df)} countries from {csv_path}")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        # Create directories if needed
        (base_path / 'data').mkdir(exist_ok=True)
        (base_path / 'data_backup').mkdir(exist_ok=True)
        
        # Load to DuckDB
        logger.info("Loading data to DuckDB...")
        con = duckdb.connect(str(db_path))
        
        # Create schema if it doesn't exist
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Load data to raw schema
        con.execute("DROP TABLE IF EXISTS raw.country_metadata")
        con.execute("""
            CREATE TABLE raw.country_metadata AS 
            SELECT * FROM df
        """)
        
        row_count = con.execute("SELECT COUNT(*) FROM raw.country_metadata").fetchone()[0]
        logger.info(f"Loaded {row_count} rows to raw.country_metadata in DuckDB")
        
        # Show sample
        sample = con.execute("""
            SELECT country_name, iso3, continent, region, income_group 
            FROM raw.country_metadata 
            LIMIT 5
        """).fetchdf()
        logger.info(f"Sample data:\n{sample}")
        
        # Show group counts
        groups = con.execute("""
            SELECT 
                SUM(EU) as EU_members,
                SUM(OECD) as OECD_members,
                SUM(ASEAN) as ASEAN_members,
                SUM(BRICS) as BRICS_members,
                SUM(G20) as G20_members,
                SUM(FCS) as FCS_countries
            FROM raw.country_metadata
        """).fetchdf()
        logger.info(f"Group membership counts:\n{groups}")
        
        con.close()
        
        # Create backup copy
        df.to_csv(backup_path, index=False)
        logger.info(f"Saved backup CSV to {backup_path}")
        
        logger.info("Country metadata ingestion completed successfully")
        return True
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        return False
    except duckdb.Error as e:
        logger.error(f"Database error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during seed ingestion: {str(e)}")
        return False

if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run ingestion
    success = run_seed_ingestion()
    if success:
        logger.info("Script completed successfully")
    else:
        logger.error("Script failed")
        exit(1)
