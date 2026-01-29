# Global Macro Analysis Data Pipeline

Data engineering pipeline for collecting, validating, transforming, and analyzing global macroeconomic indicators from World Bank, IMF, and UNDP HDR.

## Architecture

- **Orchestration**: Apache Airflow with CeleryExecutor
- **Data Warehouse**: DuckDB
- **Transformation**: dbt (staging → marts)
- **Data Quality**: Soda Core
- **Container Platform**: Docker Compose

## Pipeline Overview

**Ingestion Layer (Airflow DAGs)**
- Fetch data from World Bank, IMF, UNDP HDR APIs
- Load country metadata from CSV
- Store raw data in DuckDB `raw` schema

**Transformation Layer (dbt)**
- **Staging models**: Clean and standardize raw data (4 views)
- **Marts models**: Analytics-ready dimensional model (3 tables)
  - `dim_country`: Country dimension
  - `fact_macro_indicators`: 66 indicators in wide format
  - `mart_country_profiles`: Latest year snapshot

**Quality Layer (Soda)**
- Row counts, duplicates, NULL thresholds
- Value range validations
- Schema integrity checks

## Quick Start

### 1. Prerequisites

- Docker Desktop installed and running
- At least 4GB RAM allocated to Docker

### 2. Initial Setup

```powershell
# Clone the repository
git clone <your-repo-url>
cd global_macro_analysis

# Copy environment template and configure
copy .env.example .env
# Edit .env and add your HDR_API_KEY

# Directories are already created, but if needed:
# mkdir airflow/logs airflow/plugins airflow/config data_backups
```

### 3. Build and Start Services

```powershell
# Build custom Airflow image with dependencies
docker-compose build

# Initialize Airflow database
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

### 4. Access Airflow

- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

### 5. Run Data Pipeline

**Step 1: Run Ingestion DAGs** (trigger manually from Airflow UI):
1. **`load_seed_data`** - Load country metadata (run FIRST)
2. **`fetch_wb_data`** - World Bank indicators (35 indicators)
3. **`fetch_hdr_data`** - UNDP HDR indicators (23 indicators)
4. **`fetch_imf_data`** - IMF WEO + FDI indicators (5 indicators)

**Step 2: Run Transformation DAG**:
5. **`dbt_soda_transform`** - Run dbt models and Soda quality checks
   - Task 1: `dbt_run_models` - Create staging views and marts tables
   - Task 2: `dbt_test_models` - Run dbt tests
   - Task 3: `soda_scan_staging` - Validate staging data
   - Task 4: `soda_scan_marts` - Validate marts data

All DAGs are set to manual trigger (no automatic sch (5 DAGs)
│   ├── logs/              # Airflow logs
│   ├── config/            # Airflow configuration
│   └── plugins/           # Custom Airflow plugins
├── data/
│   ├── warehouse.duckdb   # DuckDB database (raw + marts schemas)
│   └── seeds/
│       └── country_continent_group.csv
├── data_backups/          # CSV backups of raw data
├── dbt/                   # dbt transformation project
│   ├── models/
│   │   ├── staging/       # 4 staging views
│   │   └── marts/         # 3 marts tables
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml       # dbt_utils dependency
├── soda/                  # Soda data quality checks
│   ├── checks/            # 5 YAML check files
│   ├── configuration.yml  # Staging schema config
│   └── configuration_marts.yml  # Marts schema config
├── scripts/               # Data fetching scripts
│   ├── fetch_wb_api.py
│   ├── fetch_hdr_api.py
│   ├── fetch_imf_api.py
│   └── load_country_metadata.py
├── docker-compose.ya_metadata.py
│   └── notebooks/        # Exploratory notebooks
├── dbt/                   # dbt project (to be implemented)
├── data/
│   ├── seeds/
│   │   └── country_continent_group.csv
│   └── warehouse.duckdb  # Created after first DAG run
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```5 indicators)
- WEO: Public debt, primary balance, current account, PPP conversion rate
- FDI: Financial Development Index
- Time frame: 26-year rolling window (2000-2026)
- **Note**: FDI uses ISO2 codes, converted to ISO3 via country_metadata join
### World Bank (35 indicators)
- GDP metrics, inflation, trade, employment, inequality, education, health spending
- Time frame: 26-year rolling window (2000-2026)

### IMF (8 indicators)
- WEO: Public debt, primary balance, investment, current account, PPP, trade balance, unemployment
- FDI: Financial Development index
- Time frame: 26-year rolling window (2000-2026)

### UNDP HDR (23 indicators)
- HDI, GII, GDI, MPI, inequality indices, education, life expectancy, labor force participation
- Time frame: 26-year rolling window (2000-2026)

### Country Metadata
- Geographic groupings (continent, region)
- Economic classification (income group)
- Membership flags (EU, OECD, ASEAN, BRICS, G20, FCS)

## DuckDB Schema

### Raw Layer (`raw` schema)
- `raw.wb_indicators` - World Bank data (pivoted, indicators as columns)
- `raw.imf_weo_indicators` - IMF WEO data (pivoted, indicators as columns)
- `raw.imf_fdi_indicator` - IMF FDI data
- `raw.hdr_indicators` - UNDP HDR data (pivoted, indicators as columns)
- `raw.country_metadata` - Country dimension table

## Useful Commands

```powershell
# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Stop services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# Restart a specific service
docker-compose restart airflow-scheduler

# Execute commands in Airflow container
docker-compose exec airflow-webserver bash

# Query DuckDB directly
docker-compose exec airflow-webserver python -c "import duckdb; con = duckdb.connect('/opt/airflow/data/warehouse.duckdb'); print(con.execute('SHOW TABLES').fetchdf())"
```

## Next Steps

1. ✅ Data ingestion scripts with Pydantic validation
2. ✅ DuckDB warehouse setup
3. ✅ Airflow orchestration (Docker Compose)
4. ✅ Production-ready DAGs with error handling
5. ⏳ dbt transformation layer (staging, marts)
6. ⏳ Data quality tests
7. ⏳ Metabase dashboards for visualization

## Querying the Data

### Option 1: Python Script (Recommended)
```powershell
# From project root with venv activated
python query_db.py

# Custom queries
python query_db.py "SELECT * FROM raw.country_metadata LIMIT 5"
```

### Option 2: DuckDB CLI
```powershell
# Install DuckDB CLI, then:
cd data
duckdb warehouse.duckdb

# In DuckDB:
SHOW TABLES;
SELECT * FROM raw.wb_indicators LIMIT 5;
```

## Troubleshooting

### Permission Issues
If you encounter permission errors on Linux/Mac:
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Port Conflicts
If port 8080 is already in use, modify `docker-compose.yml`:
```yaml
ports:
  - "8081:8080"  # Change host port
```

### Memory Issues
Increase Docker Desktop memory allocation to at least 4GB in Docker Desktop Settings.
