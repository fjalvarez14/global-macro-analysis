import pandas as pd
import requests
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
import duckdb
from pathlib import Path
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Path configuration
def get_base_path():
    """Returns base path depending on environment (Docker or local)"""
    if os.path.exists('/opt/airflow'):
        return Path('/opt/airflow')
    return Path(__file__).parent.parent

# Pydantic Models for validation

class HDRDataPoint(BaseModel):
    """Model for individual HDR data point from API"""
    countryIsoCode: str = Field(..., min_length=2, max_length=3)
    country: str
    year: int = Field(..., ge=1990, le=2030)
    indicatorCode: str
    value: Optional[float] = None
    
    @field_validator('value')
    @classmethod
    def validate_value(cls, v):
        if v is not None and (v < -100 or v > 1000):
            raise ValueError(f'Value {v} out of reasonable range')
        return v


class HDRPivotedRow(BaseModel):
    """Model for pivoted data row with all indicators"""
    countryIsoCode: str
    country: str
    year: int
    ineq_inc: Optional[float] = None
    ineq_edu: Optional[float] = None
    ineq_le: Optional[float] = None
    gdi: Optional[float] = None
    coef_ineq: Optional[float] = None
    mys: Optional[float] = None
    le: Optional[float] = None
    hdi: Optional[float] = None
    eys: Optional[float] = None
    gii: Optional[float] = None
    lfpr_m: Optional[float] = None
    lfpr_f: Optional[float] = None
    mpi_value: Optional[float] = None
    nutrition: Optional[float] = None
    child_mortality: Optional[float] = None
    years_of_schooling: Optional[float] = None
    school_attendance: Optional[float] = None
    cooking_fuel: Optional[float] = None
    sanitation: Optional[float] = None
    drinking_water: Optional[float] = None
    electricity: Optional[float] = None
    housing: Optional[float] = None
    assets: Optional[float] = None


# 23 indicators - Yearly - HDR 

indicators = [    
    "ineq_inc",	                 # Inequality in income
    "ineq_edu",	                 # Inequality in eduation
    "ineq_le",	                 # Inequality in life expectancy
    "gdi",	                     # Gender Development Index (value)
    "coef_ineq",	             # Coefficient of human inequality
    "mys",	                     # Mean Years of Schooling (years)
    "le",	                     # Life Expectancy at Birth (years)
    "hdi",	                     # Human Development Index (value)
    "eys",	                     # Expected Years of Schooling (years)
    "gii",	                     # Gender Inequality Index (value)
    "lfpr_m",	                 # Labour force participation rate, male (% ages 15 and older)
    "lfpr_f",	                 # Labour force participation rate, female (% ages 15 and older)
    "mpi_value",	             # MPI Value (Range: 0 to 1)
    "nutrition",	             # Contribution of Nutrition to MPI (%)
    "child_mortality",	         # Contribution of Child mortality to MPI (%)
    "years_of_schooling",	     # Contribution of Years of schooling to MPI (%)
    "school_attendance",	     # Contribution of School attendance to MPI (%)
    "cooking_fuel",	             # Contribution of Cooking fuel to MPI (%)
    "sanitation",	             # Contribution of Sanitation to MPI (%)
    "drinking_water",	         # Contribution of Drinking water to MPI (%)
    "electricity",	             # Contribution of Electricity to MPI (%)
    "housing",	                 # Contribution of Housing to MPI (%)
    "assets"	                 # Contribution of Assets to MPI (%)
]


# Fetch data from HDR API
def fetch_hdr_data(indicators, start_year, end_year, apikey):
    """Fetch HDR data from API with validation"""
    indicator_string = ",".join(indicators)
    years = ",".join(str(year) for year in range(start_year, end_year))
    
    url = f"https://hdrdata.org/api/CompositeIndices/query-detailed?apikey={apikey}&year={years}&indicator={indicator_string}"
    
    logger.info(f"Fetching {len(indicators)} indicators for years {start_year}-{end_year-1}...")
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    
    # Validate data points with Pydantic
    validated_data = []
    validation_errors = []
    for item in data:
        try:
            validated_item = HDRDataPoint(**item)
            validated_data.append(validated_item.model_dump())
        except Exception as e:
            validation_errors.append(f"Error in {item.get('country', 'Unknown')}: {str(e)}")
    
    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation errors (showing first 5):")
        for err in validation_errors[:5]:
            logger.warning(f"  - {err}")
    
    logger.info(f"Validated {len(validated_data)} data points successfully")
    df = pd.DataFrame(validated_data)
    logger.info(f"Data fetched from HDR API: {len(df)} rows")
    return df


def pivot_and_validate(df_hdr):
    """Pivot HDR data and validate with Pydantic"""
    df_hdr_pivoted = df_hdr.pivot_table(
        index=['countryIsoCode', 'country', 'year'],
        columns='indicatorCode',
        values='value',
        aggfunc='first'
    ).reset_index()
    
    df_hdr_pivoted.columns.name = None
    
    validated_rows = []
    validation_errors = []
    for idx, row in df_hdr_pivoted.iterrows():
        try:
            validated_row = HDRPivotedRow(**row.to_dict())
            validated_rows.append(validated_row.model_dump())
        except Exception as e:
            validation_errors.append(f"Row {idx} ({row.get('country', 'Unknown')}): {str(e)}")
    
    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation errors in pivoted data (showing first 5):")
        for err in validation_errors[:5]:
            logger.warning(f"  - {err}")
    
    logger.info(f"Validated {len(validated_rows)} pivoted rows successfully")
    df_hdr_final = pd.DataFrame(validated_rows)
    logger.info(f"Pivoted DataFrame shape: {df_hdr_final.shape}")
    
    return df_hdr_final


def run_hdr_ingestion(start_year=None, end_year=None):
    """Main function to run HDR data ingestion for Airflow
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get API key from environment
        apikey = os.getenv('HDR_API_KEY')
        if not apikey:
            logger.error("HDR_API_KEY environment variable not set")
            return False
        
        # Set time frame
        start_year = 2000
        end_year = datetime.now().year
        
        # Fetch data from API
        df_hdr = fetch_hdr_data(indicators, start_year, end_year, apikey)
        
        if df_hdr.empty:
            logger.error("No data fetched from HDR API")
            return False
        
        # Pivot and validate
        df_hdr_final = pivot_and_validate(df_hdr)
        
        # Get paths
        base_path = get_base_path()
        data_path = base_path / 'data'
        backup_path = base_path / 'data_backup'
        
        # Create directories if they don't exist
        data_path.mkdir(exist_ok=True)
        backup_path.mkdir(exist_ok=True)
        
        # Load to DuckDB
        logger.info("Loading data to DuckDB...")
        db_path = data_path / 'warehouse.duckdb'
        con = duckdb.connect(str(db_path))
        
        # Create schema if it doesn't exist
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Load data to raw schema
        con.execute("DROP TABLE IF EXISTS raw.hdr_indicators")
        con.execute("""
            CREATE TABLE raw.hdr_indicators AS 
            SELECT * FROM df_hdr_final
        """)
        
        row_count = con.execute("SELECT COUNT(*) FROM raw.hdr_indicators").fetchone()[0]
        logger.info(f"Loaded {row_count} rows to raw.hdr_indicators in DuckDB")
        
        con.close()
        
        # Export CSV backup
        backup_file = backup_path / f"hdr_indicators_{start_year}_{end_year}.csv"
        df_hdr_final.to_csv(backup_file, index=False)
        logger.info(f"Saved backup CSV to {backup_file}")
        logger.info(f"Total rows: {len(df_hdr_final)}, Total columns: {len(df_hdr_final.columns)}")
        
        logger.info("HDR data ingestion completed successfully")
        return True
        
    except requests.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        return False
    except duckdb.Error as e:
        logger.error(f"Database error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during HDR ingestion: {str(e)}")
        return False


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run ingestion
    success = run_hdr_ingestion()
    if success:
        logger.info("Script completed successfully")
    else:
        logger.error("Script failed")
        exit(1)
