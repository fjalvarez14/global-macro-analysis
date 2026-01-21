import pandas as pd
from datetime import datetime
import requests
import time
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Union
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

class IMFWEODataPoint(BaseModel):
    """Model for individual IMF WEO data point"""
    country_code: str = Field(..., min_length=2, max_length=3)
    indicator: str
    year: Union[int, str]  # API returns string, we'll validate and convert
    value: Optional[float] = None
    
    @field_validator('year')
    @classmethod
    def validate_year(cls, v):
        year_int = int(v) if isinstance(v, str) else v
        if year_int < 1990 or year_int > 2030:
            raise ValueError(f'Year {year_int} out of valid range')
        return str(year_int)  # Keep as string for consistency
    

class FDIDataPoint(BaseModel):
    """Model for IMF FDI data point"""
    country_code: str = Field(..., min_length=2, max_length=3)
    year: int = Field(..., ge=1990, le=2030)
    FD_FD_IX: Optional[float] = None
    
    @field_validator('country_code')
    @classmethod
    def validate_country_code(cls, v):
        # Skip aggregate codes
        if v.startswith("1C_") or "ALLC" in v:
            raise ValueError(f'Aggregate country code {v} not allowed')
        return v
    
    @field_validator('FD_FD_IX')
    @classmethod
    def validate_fdi_index(cls, v):
        if v is not None and (v < 0 or v > 1):
            raise ValueError(f'FDI index {v} must be between 0 and 1')
        return v

class IMFPivotedRow(BaseModel):
    """Model for pivoted IMF WEO data row"""
    country_code: str
    year: str
    GGXWDG_NGDP: Optional[float] = None  # Gross public debt
    GGXCNL_NGDP: Optional[float] = None  # Primary balance
    NI_GDP: Optional[float] = None       # Total Investment
    BCA: Optional[float] = None          # Current account
    PPPEX: Optional[float] = None        # PPP conversion rate
    BT_GDP: Optional[float] = None       # Trade Balance
    LUR: Optional[float] = None          # Unemployment rate
    


#  7 indicator - Yearly - IMF WEO
imf_indicators = [ 
    'GGXWDG_NGDP',           # Gross public debt - WEO
    'GGXCNL_NGDP',           # Primary balance - WEO
    'NI_GDP',                # Total Investment (% of GDP)
    'BCA',                   # Current account (absolute)
    'PPPEX',                 # Implied PPP conversion rate
    'BT_GDP',                # Trade Balance (% of GDP)
    'LUR'                    # Unemployment rate                                   
]


# Fetch from IMF WEO JSON API
def fetch_imf_weo_json(indicators, start_year, end_year):
    base_url = "https://www.imf.org/external/datamapper/api/v1"
    
    all_data = []
    validation_errors = []
    
    logger.info(f"Indicators: {indicators}")
    logger.info(f"Time frame: {start_year} to {end_year-1}")
    
    for indicator in indicators:
        try:
            # Construct URL for each indicator
            url = f"{base_url}/{indicator}"
            logger.info(f"Fetching {indicator}...")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise exception for bad status codes
            
            data = response.json()
            
            # Parse the JSON structure (varies by indicator)
            if 'values' in data:
                for country, years in data['values'][indicator].items():
                    for year, value in years.items():
                        # Filter by time frame
                        year_int = int(year)
                        if start_year <= year_int < end_year:
                            # Validate with Pydantic
                            try:
                                data_point = IMFWEODataPoint(
                                    country_code=country,
                                    indicator=indicator,
                                    year=year,
                                    value=value
                                )
                                all_data.append(data_point.model_dump())
                            except Exception as e:
                                validation_errors.append(f"{country}/{year}/{indicator}: {str(e)}")
            
            time.sleep(0.5)  
            
        except Exception as e:
            logger.error(f"Error fetching {indicator}: {e}")
            continue
    
    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation errors (showing first 5):")
        for err in validation_errors[:5]:
            logger.warning(f"  - {err}")
    
    if all_data:
        logger.info(f"Validated {len(all_data)} data points successfully")
        df = pd.DataFrame(all_data)
        return df
    else:
        logger.error("No data fetched")
        return pd.DataFrame()


# Fetch FDI data from IMF FDI dataset

def fetch_fdi_all_countries(start_year, end_year):
    
    search_url = f"https://api.db.nomics.world/v22/series/IMF/FDI?observations=1&dimensions=%7B%22INDICATOR%22:%5B%22FD_FD_IX%22%5D,%22FREQ%22:%5B%22A%22%5D%7D"
    
    r = requests.get(search_url, timeout=30)
    r.raise_for_status()
    data = r.json()
    
    series_list = data["series"]["docs"]
    
    rows = []
    validation_errors = []
    
    for series_dict in series_list:
        country_code = series_dict["dimensions"]["REF_AREA"]
        periods = series_dict["period"]
        values = series_dict["value"]
        
        for period, value in zip(periods, values):
            year = int(period)
            if start_year <= year < end_year and value is not None:
                # Validate with Pydantic
                try:
                    fdi_point = FDIDataPoint(
                        country_code=country_code,
                        year=year,
                        FD_FD_IX=value
                    )
                    rows.append(fdi_point.model_dump())
                except Exception as e:
                    validation_errors.append(f"{country_code}/{year}: {str(e)}")
    
    if validation_errors:
        logger.warning(f"Filtered out {len(validation_errors)} invalid entries (aggregates/outliers)")
    
    df = pd.DataFrame(rows)
    logger.info(f"Validated and extracted data for {df['country_code'].nunique()} countries")
    return df


def pivot_and_validate_weo(df_imf):
    """Pivot IMF WEO data and validate with Pydantic"""
    df_imf_pivoted = df_imf.pivot_table(
        index=['country_code', 'year'],
        columns='indicator',
        values='value',
        aggfunc='first'
    ).reset_index()
    
    df_imf_pivoted.columns.name = None
    
    validated_rows = []
    validation_errors = []
    for idx, row in df_imf_pivoted.iterrows():
        try:
            validated_row = IMFPivotedRow(**row.to_dict())
            validated_rows.append(validated_row.model_dump())
        except Exception as e:
            validation_errors.append(f"Row {idx} ({row.get('country_code', 'Unknown')}): {str(e)}")

    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation errors in pivoted data (showing first 5):")
        for err in validation_errors[:5]:
            logger.warning(f"  - {err}")

    logger.info(f"Validated {len(validated_rows)} pivoted rows successfully")
    df_imf_final = pd.DataFrame(validated_rows)
    logger.info(f"Pivoted DataFrame shape: {df_imf_final.shape}")
    
    return df_imf_final


def run_imf_ingestion(start_year=None, end_year=None):
    """Main function to run IMF data ingestion for Airflow

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Set time frame
        start_year = 2000
        end_year = datetime.now().year
        
        logger.info(f"Fetching {len(imf_indicators)} indicators, years {start_year}-{end_year}")
        
        # Fetch IMF WEO data
        df_imf = fetch_imf_weo_json(imf_indicators, start_year, end_year)
        
        if df_imf.empty:
            logger.error("No IMF WEO data fetched")
            return False
        
        logger.info(f"IMF WEO DataFrame shape: {df_imf.shape}")
        logger.info(f"Years available: {sorted(df_imf['year'].unique())}")
        logger.info(f"Indicators: {df_imf['indicator'].unique()}")
        
        # Fetch FDI data
        df_fdi = fetch_fdi_all_countries(start_year, end_year)
        
        if df_fdi.empty:
            logger.warning("No FDI data fetched, continuing with WEO data only")
        
        # Pivot and validate WEO data
        df_imf_final = pivot_and_validate_weo(df_imf)
        
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
        
        # Load IMF WEO data
        con.execute("DROP TABLE IF EXISTS raw.imf_weo_indicators")
        con.execute("""
            CREATE TABLE raw.imf_weo_indicators AS 
            SELECT * FROM df_imf_final
        """)
        
        weo_count = con.execute("SELECT COUNT(*) FROM raw.imf_weo_indicators").fetchone()[0]
        logger.info(f"Loaded {weo_count} rows to raw.imf_weo_indicators in DuckDB")
        
        # Load IMF FDI data if available
        if not df_fdi.empty:
            con.execute("DROP TABLE IF EXISTS raw.imf_fdi_indicator")
            con.execute("""
                CREATE TABLE raw.imf_fdi_indicator AS 
                SELECT * FROM df_fdi
            """)
            
            fdi_count = con.execute("SELECT COUNT(*) FROM raw.imf_fdi_indicator").fetchone()[0]
            logger.info(f"Loaded {fdi_count} rows to raw.imf_fdi_indicator in DuckDB")
        
        con.close()
        
        # Export CSV backups
        backup_file_weo = backup_path / f"imf_weo_indicators_{start_year}_{end_year}.csv"
        df_imf_final.to_csv(backup_file_weo, index=False)
        logger.info(f"Saved backup CSV to {backup_file_weo}")
        logger.info(f"Total WEO rows: {len(df_imf_final)}")
        
        if not df_fdi.empty:
            backup_file_fdi = backup_path / f"imf_fdi_indicator_{start_year}_{end_year}.csv"
            df_fdi.to_csv(backup_file_fdi, index=False)
            logger.info(f"Saved backup CSV to {backup_file_fdi}")
            logger.info(f"Total FDI rows: {len(df_fdi)}")
        
        logger.info("IMF data ingestion completed successfully")
        return True
        
    except requests.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        return False
    except duckdb.Error as e:
        logger.error(f"Database error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during IMF ingestion: {str(e)}")
        return False


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run ingestion
    success = run_imf_ingestion()
    if success:
        logger.info("Script completed successfully")
    else:
        logger.error("Script failed")
        exit(1)
