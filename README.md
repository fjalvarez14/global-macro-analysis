# Global Macro Analysis Data Pipeline

Data engineering pipeline for collecting, validating, and analyzing global macroeconomic indicators from World Bank, IMF, and UNDP HDR.

## Architecture

- **Orchestration**: Apache Airflow
- **Data Warehouse**: DuckDB
- **Transformation**: dbt (to be implemented)
- **Visualization**: Metabase (to be implemented)
- **Container Platform**: Docker Compose

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

### 5. Run Data Ingestion

Once Airflow is running, trigger the DAGs manually from the UI:
1. **`load_seed_data`** - Run FIRST to load country metadata
2. **`fetch_wb_data`** - World Bank indicators (35 indicators)
3. **`fetch_hdr_data`** - UNDP HDR indicators (23 indicators)
4. **`fetch_imf_data`** - IMF WEO + FDI indicators (8 indicators)

All DAGs are set to manual trigger (no automatic scheduling).

## Project Structure

```
global_macro_analysis/
├── airflow/
│   ├── dags/              # Airflow DAG definitions
│   ├── logs/              # Airflow logs
│   └── plugins/           # Custom Airflow plugins
├── data/
│   └── warehouse.duckdb   # DuckDB database file
├── data_backup/           # CSV backups of raw data
├── scripts/               # Data fetching scripts (production-ready)
│   ├── fetch_wb_api.py
│   ├── fetch_hdr_api.py
│   ├── fetch_imf_api.py
│   ├── load_country_metadata.py
│   └── notebooks/        # Exploratory notebooks
├── dbt/                   # dbt project (to be implemented)
├── data/
│   ├── seeds/
│   │   └── country_continent_group.csv
│   └── warehouse.duckdb  # Created after first DAG run
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Data Sources

### World Bank (35 indicators)
- GDP metrics, inflation, trade, employment, inequality, education, health spending
- Time frame: 26-year rolling window (2000-2026)

### IMF (8 indicators)
- WEO: Public debt, primary balance, investment, current account, PPP, trade balance, unemployment
- FDI: Foreign Direct Investment index
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
