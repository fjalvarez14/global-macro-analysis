import wbgapi as wb
import pandas as pd
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
import duckdb
from pathlib import Path
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logger = logging.getLogger(__name__)

# Path configuration
def get_base_path():
    """Returns base path depending on environment (Docker or local)"""
    if os.path.exists('/opt/airflow'):
        return Path('/opt/airflow')
    return Path(__file__).parent.parent

# Pydantic Models for validation

class WBDataPoint(BaseModel):
    """Model for individual World Bank data point"""
    country_code: str = Field(..., min_length=2, max_length=3)
    Country: str
    year: int = Field(..., ge=1990, le=2030)
    indicator: str
    value: Optional[float] = None

class WBPivotedRow(BaseModel):
    """Model for pivoted World Bank data row with all indicators"""
    model_config = {'populate_by_name': True}  
    
    country_code: str
    Country: str
    year: int
    NY_GDP_MKTP_CD: Optional[float] = Field(None, alias='GDP (current US$)')
    NY_GDP_MKTP_KD: Optional[float] = Field(None, alias='GDP (constant 2015 US$)')
    NY_GDP_MKTP_PP_CD: Optional[float] = Field(None, alias='GDP, PPP (current international $)')
    NY_GDP_PCAP_PP_CD: Optional[float] = Field(None, alias='GDP per capita, PPP (current international $)')
    NY_GDP_PCAP_CD: Optional[float] = Field(None, alias='GDP per capita (current US$)')
    NY_GDP_MKTP_KD_ZG: Optional[float] = Field(None, alias='GDP growth (annual %)')
    FP_CPI_TOTL_ZG: Optional[float] = Field(None, alias='Inflation, consumer prices (annual %)')
    NY_GDP_DEFL_KD_ZG: Optional[float] = Field(None, alias='Inflation, GDP deflator (annual %)')
    FR_INR_RINR: Optional[float] = Field(None, alias='Real interest rate (%)')
    NE_GDI_TOTL_ZS: Optional[float] = Field(None, alias='Gross capital formation (% of GDP)')
    NV_AGR_TOTL_ZS: Optional[float] = Field(None, alias='Agriculture, forestry, and fishing, value added (% of GDP)')
    NV_IND_TOTL_ZS: Optional[float] = Field(None, alias='Industry (including construction), value added (% of GDP)')
    NV_SRV_TOTL_ZS: Optional[float] = Field(None, alias='Services, value added (% of GDP)')
    NE_EXP_GNFS_ZS: Optional[float] = Field(None, alias='Exports of goods and services (% of GDP)')
    NE_IMP_GNFS_ZS: Optional[float] = Field(None, alias='Imports of goods and services (% of GDP)')
    BN_CAB_XOKA_GD_ZS: Optional[float] = Field(None, alias='Current account balance (% of GDP)')
    DT_DOD_DECT_GN_ZS: Optional[float] = Field(None, alias='External debt stocks (% of GNI)')
    FI_RES_TOTL_CD: Optional[float] = Field(None, alias='Total reserves (includes gold, current US$)')
    GC_DOD_TOTL_GD_ZS: Optional[float] = Field(None, alias='Central government debt, total (% of GDP)')
    GC_REV_XGRT_GD_ZS: Optional[float] = Field(None, alias='Revenue, excluding grants (% of GDP)')
    GC_TAX_TOTL_GD_ZS: Optional[float] = Field(None, alias='Tax revenue (% of GDP)')
    FS_AST_PRVT_GD_ZS: Optional[float] = Field(None, alias='Domestic credit to private sector (% of GDP)')
    NE_TRD_GNFS_ZS: Optional[float] = Field(None, alias='Trade (% of GDP)')
    SP_POP_TOTL: Optional[float] = Field(None, alias='Population, total')
    SL_UEM_TOTL_ZS: Optional[float] = Field(None, alias='Unemployment, total (% of total labor force) (modeled ILO estimate)')
    SL_GDP_PCAP_EM_KD: Optional[float] = Field(None, alias='GDP per person employed (constant 2021 PPP $)')
    SL_TLF_CACT_ZS: Optional[float] = Field(None, alias='Labor force participation rate, total (% of total population ages 15+) (modeled ILO estimate)')
    SI_POV_GINI: Optional[float] = Field(None, alias='Gini index')
    SI_POV_DDAY: Optional[float] = Field(None, alias='Poverty headcount ratio at $2.15 a day (2017 PPP) (% of population)')
    SP_DYN_LE00_IN: Optional[float] = Field(None, alias='Life expectancy at birth, total (years)')
    SE_SEC_ENRR: Optional[float] = Field(None, alias='School enrollment, secondary (% gross)')
    SE_XPD_TOTL_GD_ZS: Optional[float] = Field(None, alias='Government expenditure on education, total (% of GDP)')
    SH_XPD_CHEX_GD_ZS: Optional[float] = Field(None, alias='Current health expenditure (% of GDP)')
    SH_XPD_GHED_CH_ZS: Optional[float] = Field(None, alias='Domestic general government health expenditure (% of current health expenditure)')
    SH_XPD_GHED_GD_ZS: Optional[float] = Field(None, alias='Domestic general government health expenditure (% of GDP)')


# 35 indicators - Yearly Basis - WorldBank
indicators = [
    'NY.GDP.MKTP.CD',        # GDP (current US$)
    'NY.GDP.MKTP.KD',        # GDP (constant 2015 US$)
    'NY.GDP.MKTP.PP.CD',     # GDP (current PPP, international $)
    'NY.GDP.PCAP.PP.CD',     # GDP per capita, PPP (current international $)
    'NY.GDP.PCAP.CD',        # GDP per capita (current US$)
    'NY.GDP.MKTP.KD.ZG',     # GDP growth (annual %)
    'FP.CPI.TOTL.ZG',        # Inflation, consumer prices (annual %)
    'NY.GDP.DEFL.KD.ZG',     # Inflation, GDP deflator (annual %)
    'FR.INR.RINR',           # Real interest rate (%)
    'NE.GDI.TOTL.ZS',        # Gross capital formation (% of GDP)
    'NV.AGR.TOTL.ZS',        # Agriculture, forestry, and fishing, value added (% of GDP)
    'NV.IND.TOTL.ZS',        # Industry (including construction), value added (% of GDP)
    'NV.SRV.TOTL.ZS',        # Services, value added (% of GDP)
    'NE.EXP.GNFS.ZS',        # Exports of goods and services (% of GDP)
    'NE.IMP.GNFS.ZS',        # Imports of goods and services (% of GDP)
    'BN.CAB.XOKA.GD.ZS',     # Current account balance (% of GDP)
    'DT.DOD.DECT.GN.ZS',     # External debt stocks (% of GNI)
    'FI.RES.TOTL.CD',        # Total reserves (includes gold, current US$)
    'GC.DOD.TOTL.GD.ZS',     # Central government debt, total (% of GDP)
    'GC.REV.XGRT.GD.ZS',     # Revenue, excluding grants (% of GDP)
    'GC.TAX.TOTL.GD.ZS',     # Tax revenue (% of GDP)
    'FS.AST.PRVT.GD.ZS',     # Domestic credit to private sector (% of GDP)
    'NE.TRD.GNFS.ZS',        # Trade (% of GDP)
    'SP.POP.TOTL',           # Population
    'SL.UEM.TOTL.ZS',        # Unemployment, total (% of total labor force) (modeled ILO estimate)
    'SL.GDP.PCAP.EM.KD',     # GDP per person employed (constant 2021 PPP $)
    'SL.TLF.CACT.ZS',        # Labor force participation rate, total (% of total population ages 15+) (modeled ILO estimate)
    'SI.POV.GINI',           # GINI index
    'SI.POV.DDAY',           # Poverty headcount ratio at $3.00 a day (2021 PPP) (% of population)
    'SP.DYN.LE00.IN',        # Life expectancy at birth, total (years)
    'SE.SEC.ENRR',           # School enrollment, secondary (% gross)
    'SE.XPD.TOTL.GD.ZS',     # Government expenditure on education, total (% of GDP)
    'SH.XPD.CHEX.GD.ZS',     # Current health expenditure (% of GDP)
    'SH.XPD.GHED.CH.ZS',     # Domestic general government health expenditure (% of current health expenditure)
    'SH.XPD.GHED.GD.ZS'      # Domestic general government health expenditure (% of GDP)
]


# Helper function for parallel fetching
def _fetch_indicator_chunk(chunk_indicators, countries, years, chunk_num, total_chunks):
    """Fetch a subset of indicators - used for parallel processing"""
    try:
        logger.info(f"Fetching chunk {chunk_num}/{total_chunks} ({len(chunk_indicators)} indicators)...")
        df = wb.data.DataFrame(
            chunk_indicators,
            countries,
            time=years,
            labels=True,
            numericTimeKeys=True,
            skipBlanks=True,
            skipAggs=True
        )
        df = df.reset_index()
        logger.info(f"Chunk {chunk_num}/{total_chunks} completed: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error fetching chunk {chunk_num}: {str(e)}")
        return pd.DataFrame()


# Fetch World Bank data
def fetch_wb_data(indicators, start_year, end_year):
    """DataFrame with validated long-format data from World Bank API using parallel chunked requests"""
    years = list(range(start_year, end_year))
    countries = wb.region.members('WLD')
    
    # Split indicators into chunks for parallel fetching
    chunk_size = 7
    indicator_chunks = [indicators[i:i + chunk_size] for i in range(0, len(indicators), chunk_size)]
    total_chunks = len(indicator_chunks)
    
    logger.info(f"Fetching {len(indicators)} indicators in {total_chunks} parallel chunks for years {start_year}-{end_year-1}...")
    
    # Fetch chunks in parallel
    all_dfs = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_chunk = {
            executor.submit(_fetch_indicator_chunk, chunk, countries, years, idx+1, total_chunks): idx 
            for idx, chunk in enumerate(indicator_chunks)
        }
        
        for future in as_completed(future_to_chunk):
            df_chunk = future.result()
            if not df_chunk.empty:
                all_dfs.append(df_chunk)
    
    if not all_dfs:
        logger.error("No data retrieved from any chunks")
        return pd.DataFrame()
    
    df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Combined {len(all_dfs)} chunks: {len(df)} total observations")
    
    df = df.rename(columns={'economy': 'country_code', 'Series': 'indicator'})
    
    # Transform year columns into rows
    year_columns = [col for col in df.columns if isinstance(col, int)]
    id_columns = ['country_code', 'Country', 'indicators']
    
    df_long = df.melt(
        id_vars=id_columns,
        value_vars=year_columns,
        var_name='year',
        value_name='value'
    )
    
    # Validate data points with Pydantic
    validated_data = []
    validation_errors = []
    
    for idx, row in df_long.iterrows():
        try:
            data_point = WBDataPoint(
                country_code=row['country_code'],
                Country=row['Country'],
                year=row['year'],
                indicator=row['indicators'],
                value=row['value']
            )
            validated_data.append(data_point.model_dump())
        except Exception as e:
            validation_errors.append(f"Row {idx}: {str(e)}")
    
    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation errors (showing first 5):")
        for err in validation_errors[:5]:
            logger.warning(f"  - {err}")
    
    logger.info(f"Validated {len(validated_data)} data points successfully")
    df_validated = pd.DataFrame(validated_data)
    
    logger.info(f"Long format DataFrame shape: {df_validated.shape}")
    
    return df_validated


def pivot_and_validate(df_wb):
    """Pivot WB data and validate with Pydantic"""
    df_wb_pivoted = df_wb.pivot_table(
        index=['country_code', 'Country', 'year'],
        columns='indicator',
        values='value',
        aggfunc='first'
    ).reset_index()
    
    df_wb_pivoted.columns.name = None
    
    validated_rows = []
    validation_errors = []
    for idx, row in df_wb_pivoted.iterrows():
        try:
            validated_row = WBPivotedRow(**row.to_dict())
            validated_rows.append(validated_row.model_dump())
        except Exception as e:
            validation_errors.append(f"Row {idx} ({row.get('Country', 'Unknown')}): {str(e)}")

    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation errors in pivoted data (showing first 5):")
        for err in validation_errors[:5]:
            logger.warning(f"  - {err}")

    logger.info(f"Validated {len(validated_rows)} pivoted rows successfully")
    df_wb_final = pd.DataFrame(validated_rows)
    logger.info(f"Pivoted DataFrame shape: {df_wb_final.shape}")
    
    return df_wb_final


def run_wb_ingestion(start_year=None, end_year=None):
    """Main function to run World Bank data ingestion for Airflow DAG
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Set time frame
        start_year = 2000
        end_year = datetime.now().year
        
        # Fetch data from API
        df_wb = fetch_wb_data(indicators, start_year, end_year)
        
        if df_wb.empty:
            logger.error("No data fetched from World Bank API")
            return False
        
        # Pivot and validate
        df_wb_final = pivot_and_validate(df_wb)
        
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
        con.execute("DROP TABLE IF EXISTS raw.wb_indicators")
        con.execute("""
            CREATE TABLE raw.wb_indicators AS 
            SELECT * FROM df_wb_final
        """)
        
        row_count = con.execute("SELECT COUNT(*) FROM raw.wb_indicators").fetchone()[0]
        logger.info(f"Loaded {row_count} rows to raw.wb_indicators in DuckDB")
        
        con.close()
        
        # Export CSV backup
        backup_file = backup_path / f"wb_indicators_{start_year}_{end_year}.csv"
        df_wb_final.to_csv(backup_file, index=False)
        logger.info(f"Saved backup CSV to {backup_file}")
        logger.info(f"Total rows: {len(df_wb_final)}, Total columns: {len(df_wb_final.columns)}")
        
        logger.info("World Bank data ingestion completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error during World Bank ingestion: {str(e)}")
        return False


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run ingestion
    success = run_wb_ingestion()
    if success:
        logger.info("Script completed successfully")
    else:
        logger.error("Script failed")
        exit(1)
