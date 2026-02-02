# Global Macro Analysis Data Pipeline

A production-ready data engineering pipeline for collecting, validating, transforming, and analyzing global macroeconomic indicators from World Bank, IMF, and UNDP Human Development Reports.

## 📊 Project Overview

This pipeline integrates **56 macroeconomic indicators** from three authoritative sources, covering **~195 countries** over a **26-year period (2000-2026)**. Data flows through a modern ELT architecture with automated quality checks and business intelligence visualization.

**Key Metrics**:
- 🌍 **195+ countries** with complete geographic and economic metadata
- 📈 **56 indicators** (28 World Bank + 23 HDR + 5 IMF)
- 📅 **26 years** of historical data (2000-2026)
- 🔍 **3-layer quality validation** (Pydantic → dbt tests → Soda checks)
- ⚡ **~340,000 data points** in optimized star schema

## 🏗️ Architecture

**Technology Stack**:
- **Orchestration**: Apache Airflow 3.1.6 with CeleryExecutor
- **Data Warehouse**: DuckDB 1.0+ (embedded OLAP database)
- **Transformation**: dbt-core 1.9.8 with dbt-duckdb adapter
- **Data Quality**: Soda Core 3.5.6 + dbt tests
- **Business Intelligence**: Metabase with DuckDB driver
- **Container Platform**: Docker Compose (7 services)

**Architecture Diagram**: See [docs/ARCHITECTURE_DIAGRAM.md](docs/ARCHITECTURE_DIAGRAM.md) for comprehensive system diagrams.

### Data Flow

```
External APIs → Airflow DAGs → DuckDB (raw) → dbt (staging → marts) → Soda Quality → Metabase
```

**Pipeline Layers**:
1. **Ingestion Layer** (Airflow DAGs): Fetch data from APIs, validate with Pydantic, store in `raw` schema
2. **Transformation Layer** (dbt): Clean data in `staging` views, create analytics-ready `marts` tables
3. **Quality Layer** (Soda + dbt): Validate row counts, check for duplicates, enforce constraints
4. **Visualization Layer** (Metabase): Interactive dashboards and ad-hoc SQL queries

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** installed and running ([Download](https://www.docker.com/products/docker-desktop))
- **Minimum 4GB RAM** allocated to Docker
- **HDR API Key** (free, get from [https://hdrdata.org/](https://hdrdata.org/))

### Setup Instructions

#### 1. Clone and Configure Environment

```powershell
# Clone the repository
git clone <your-repo-url>
cd global_macro_analysis

# Create environment file from template
Copy-Item .env.example .env

# Edit .env and add your HDR API key
notepad .env  # Or use your preferred editor
```

**Required in `.env`**:
```dotenv
HDR_API_KEY=your_actual_api_key_here
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

**Optional**: Configure email alerts by adding SMTP settings in `.env`:
```dotenv
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
SMTP_MAIL_FROM=your-email@gmail.com
```

#### 2. Build and Start Services

```powershell
# Build custom Airflow image with dependencies
docker-compose build

# Initialize Airflow database
docker-compose up airflow-init

# Start all services (detached mode)
docker-compose up -d

# Verify all services are running
docker-compose ps
```

**Expected Services** (7 total):
- ✅ airflow-webserver (port 8080)
- ✅ airflow-scheduler
- ✅ airflow-worker
- ✅ airflow-apiserver
- ✅ postgres (Airflow metadata)
- ✅ redis (Celery broker)
- ✅ metabase (port 3000)

#### 3. Access Airflow UI

- **URL**: [http://localhost:8080](http://localhost:8080)
- **Username**: `airflow`
- **Password**: `airflow`

#### 4. Run the Data Pipeline

Automated Pipeline

Simply trigger the ingestion DAG and everything runs automatically:

1. Navigate to DAGs page in Airflow UI
2. Find **`data_ingestion_pipeline`** DAG
3. Click ▶️ (play button) to trigger
4. Pipeline executes automatically:
   - ✅ Loads country metadata (~30 seconds)
   - ✅ Fetches WB, HDR, IMF data **in parallel** (~3-4 minutes)
   - ✅ Automatically triggers `dbt_soda_transform` DAG
5. Monitor `dbt_soda_transform` DAG separately (~2-3 minutes)
   - Creates staging views
   - Builds marts tables  
   - Runs data quality validations

**Total Pipeline Runtime**: ~5-10 minutes (parallel execution)


#### 5. Access Metabase

- **URL**: [http://localhost:3000](http://localhost:3000)
- **First-time setup**: Create admin account
- **Database connection**: 
  - Type: DuckDB
  - Path: `/metabase-data/warehouse.duckdb`
  - Schema: `marts`

---

## 📦 Data Catalog

### Data Sources
### World Bank (28 indicators)
**Source**: World Bank Open Data API  
**Coverage**: ~195 countries, 2000-2026  
**Update Frequency**: Quarterly

| Category | Indicators | Count |
|----------|-----------|-------|
| **GDP & Growth** | GDP (constant, PPP, per capita), GDP growth rate | 5 |
| **Prices & Interest** | Inflation (consumer), real interest rate | 2 |
| **Investment** | Gross capital formation | 1 |
| **Sectoral** | Agriculture, industry, services value added | 3 |
| **Trade** | Exports, imports, current account balance, total trade, reserves | 5 |
| **Fiscal** | Tax revenue, domestic credit to private sector | 2 |
| **Labor** | Population, unemployment, labor force participation, GDP per employed person | 4 |
| **Inequality** | GINI index, poverty headcount | 2 |
| **Social** | School enrollment (secondary), education expenditure, health expenditure | 4 |

### UNDP Human Development Reports (23 indicators)
**Source**: UNDP HDR API  
**Coverage**: ~190 countries, 2000-2026  
**Update Frequency**: Annual

| Category | Indicators | Count |
|----------|-----------|-------|
| **Composite Indices** | HDI, GII, GDI, MPI, Inequality coefficient | 5 |
| **Inequality Dimensions** | Income, education, life expectancy inequality | 3 |
| **Education** | Mean years schooling, expected years schooling | 2 |
| **Health** | Life expectancy at birth | 1 |
| **Labor** | Labor force participation (male, female) | 2 |
| **Multidimensional Poverty** | Nutrition, child mortality, schooling, school attendance, cooking fuel, sanitation, water, electricity, housing, assets | 10 |

### IMF (5 indicators)
**Source**: IMF World Economic Outlook (WEO) + Financial Development Index (FDI) APIs  
**Coverage**: ~190 countries, 2000-2026  
**Update Frequency**: Biannual (WEO), Annual (FDI)

| Source | Indicators | Count |
|--------|-----------|-------|
| **WEO** | Public debt (% GDP), primary balance (% GDP), current account balance, PPP conversion rate | 4 |
| **FDI** | Financial Development Index | 1 |

### Country Metadata
**Source**: Manual curation + World Bank classifications  
**Coverage**: 195+ countries

| Type | Fields |
|------|--------|
| **Identifiers** | Country code (ISO3), country name, ISO2 |
| **Geographic** | Continent, region |
| **Economic** | Income group (Low, Lower-middle, Upper-middle, High) |
| **Memberships** | EU, OECD, ASEAN, BRICS, G20, FCS (Fragile and Conflict-affected States) |

---

## 🗄️ Database Schema

### Raw Layer (`raw` schema)
Immutable source data from APIs:
- `raw.country_metadata` - Country dimension data
- `raw.wb_indicators` - World Bank indicators (wide format)
- `raw.hdr_indicators` - HDR indicators (wide format)
- `raw.imf_weo_indicators` - IMF WEO indicators (wide format)
- `raw.imf_fdi_indicator` - IMF FDI data

### Staging Layer (`staging` schema)
Cleaned and standardized views:
- `stg_country_metadata` - Validated country data
- `stg_wb_indicators` - Cleaned World Bank data
- `stg_hdr_indicators` - Cleaned HDR data
- `stg_imf_indicators` - Combined and cleaned IMF data

### Marts Layer (`marts` schema)
Analytics-ready dimensional model:
- **`dim_country`** - Country dimension with 195+ countries
  - Primary key: `country_code`
  - Attributes: Geographic, economic, membership flags
  
- **`fact_macro_indicators`** - Star schema fact table with 56 indicators
  - Primary key: `country_code`, `year`
  - Grain: One row per country per year
  - Format: Wide (indicators as columns for BI tool efficiency)
  
- **`mart_country_indicators_wide`** - Latest year snapshot
  - Primary key: `country_code`
  - Purpose: Current state dashboards

---

## 📊 Example Queries

### Query 1: Top 10 Economies by GDP (2025)
```sql
SELECT 
    country_name,
    year,
    wb_gdp_constant_2015_usd / 1e12 AS gdp_trillions_usd,
    wb_gdp_per_capita_ppp AS gdp_per_capita_ppp,
    hdr_human_development_index AS hdi
FROM marts.fact_macro_indicators
WHERE year = 2025
  AND wb_gdp_constant_2015_usd IS NOT NULL
ORDER BY wb_gdp_constant_2015_usd DESC
LIMIT 10;
```

### Query 2: HDI Trends for BRICS Countries
```sql
SELECT 
    f.country_name,
    f.year,
    f.hdr_human_development_index AS hdi,
    f.wb_gdp_per_capita_ppp AS gdp_per_capita
FROM marts.fact_macro_indicators f
JOIN marts.dim_country d ON f.country_code = d.country_code
WHERE d.is_brics = TRUE
  AND f.year >= 2010
  AND f.hdr_human_development_index IS NOT NULL
ORDER BY f.country_name, f.year;
```

### Query 3: Income Inequality Across Continents (Latest Year)
```sql
SELECT 
    d.continent,
    AVG(f.wb_gini_index) AS avg_gini,
    AVG(f.hdr_inequality_coefficient) AS avg_inequality_coef,
    COUNT(DISTINCT f.country_code) AS country_count
FROM marts.fact_macro_indicators f
JOIN marts.dim_country d ON f.country_code = d.country_code
WHERE f.year = (SELECT MAX(year) FROM marts.fact_macro_indicators)
  AND f.wb_gini_index IS NOT NULL
GROUP BY d.continent
ORDER BY avg_gini DESC;
```

### Query 4: Correlation Between Education and GDP
```sql
SELECT 
    country_name,
    year,
    hdr_mean_years_schooling AS education_years,
    wb_gdp_per_capita_ppp AS gdp_per_capita,
    hdr_human_development_index AS hdi
FROM marts.fact_macro_indicators
WHERE year = 2025
  AND hdr_mean_years_schooling IS NOT NULL
  AND wb_gdp_per_capita_ppp IS NOT NULL
ORDER BY hdr_mean_years_schooling DESC
LIMIT 20;
```

---

## 🔍 Querying the Data

### Option 1: Metabase (Recommended for Business Users)
1. Access [http://localhost:3000](http://localhost:3000)
2. Navigate to "Browse Data" → "marts" schema
3. Click on any table to explore
4. Use "Ask a Question" for SQL queries or visual query builder

### Option 2: Python Script (Recommended for Data Scientists)
```powershell
# From project root
python -c "import duckdb; con = duckdb.connect('data/warehouse.duckdb'); print(con.execute('SELECT * FROM marts.fact_macro_indicators LIMIT 5').df())"
```

### Option 3: DuckDB CLI (Advanced)
```powershell
# Install DuckDB CLI first: https://duckdb.org/docs/installation/
cd data
duckdb warehouse.duckdb

# In DuckDB:
.tables                                    -- List all tables
.schema marts.fact_macro_indicators        -- Show table schema
SELECT COUNT(*) FROM marts.fact_macro_indicators;
SELECT * FROM marts.dim_country WHERE continent = 'Europe';
```

### Option 4: Airflow Container
```powershell
# Execute query from Airflow container
docker-compose exec airflow-webserver python -c "import duckdb; con = duckdb.connect('/opt/airflow/data/warehouse.duckdb'); print(con.execute('SHOW TABLES').fetchdf())"
```

---

## 🛠️ Useful Commands

### Docker Compose Management

```powershell
# View logs for specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose logs -f metabase

# View logs for all services
docker-compose logs -f

# Stop services
docker-compose down

# Stop and remove all data (clean slate)
docker-compose down -v

# Restart a specific service
docker-compose restart airflow-scheduler

# Rebuild and restart after code changes
docker-compose up -d --build

# Check service status
docker-compose ps

# Execute bash in Airflow container
docker-compose exec airflow-webserver bash
```

### dbt Commands (from Airflow container)

```powershell
# Enter Airflow container
docker-compose exec airflow-webserver bash

# Run dbt commands
cd /opt/airflow/dbt
dbt run --profiles-dir .                    # Run all models
dbt test --profiles-dir .                   # Run all tests
dbt docs generate --profiles-dir .          # Generate documentation
dbt docs serve --profiles-dir .             # Serve docs (port 8080 conflict)
dbt run --select staging --profiles-dir .   # Run only staging models
dbt run --select marts --profiles-dir .     # Run only marts models
```

### Soda Commands (from Airflow container)

```powershell
# Enter Airflow container
docker-compose exec airflow-webserver bash

# Run Soda checks
cd /opt/airflow/soda
soda scan -d global_macro_warehouse -c configuration_raw.yml checks_raw.yml
soda scan -d global_macro_warehouse -c configuration_marts.yml checks_marts.yml
```

---

## 📂 Project Structure

```
global_macro_analysis/
├── .env                 # Environment variables (gitignored)
├── .gitignore           # Git ignore rules
├── README.md            # This file
├── docker-compose.yaml  # Docker services configuration
├── Dockerfile           # Custom Airflow image
├── Dockerfile.metabase  # Metabase with DuckDB driver
├── requirements.txt     # Python dependencies
│
├── airflow/             # Apache Airflow
│   ├── dags/           # 6 production DAGs
│   │   ├── data_ingestion_pipeline.py  # ⭐ Master ingestion DAG (NEW)
│   │   ├── seed_data_dag.py            # Load country metadata
│   │   ├── wb_data_dag.py              # Fetch World Bank data
│   │   ├── hdr_data_dag.py             # Fetch HDR data
│   │   ├── imf_data_dag.py             # Fetch IMF data
│   │   └── dbt_soda_transform_dag.py   # Transform and validate
│   ├── logs/           # Airflow logs (gitignored)
│   ├── config/         # Airflow configuration
│   └── plugins/        # Custom Airflow plugins
│
├── data/               # Data storage
│   ├── warehouse.duckdb # DuckDB database file
│   └── seeds/
│       └── country_continent_group.csv
│
├── data_backups/       # CSV backups of fetched data
│   ├── country_continent_group.csv
│   ├── hdr_indicators_2000_2026.csv
│   ├── imf_weo_indicators_2000_2026.csv
│   ├── imf_fdi_indicator_2000_2026.csv
│   └── wb_indicators_2000_2026.csv
│
├── dbt/                # dbt transformation project
│   ├── models/
│   │   ├── sources.yml       # Source definitions
│   │   ├── staging/          # 4 staging views
│   │   │   ├── schema.yml
│   │   │   ├── stg_country_metadata.sql
│   │   │   ├── stg_wb_indicators.sql
│   │   │   ├── stg_hdr_indicators.sql
│   │   │   └── stg_imf_indicators.sql
│   │   └── marts/            # 3 marts tables
│   │       ├── schema.yml
│   │       ├── dim_country.sql
│   │       ├── fact_macro_indicators.sql
│   │       └── mart_country_indicators_wide.sql
│   ├── macros/               # Custom macros
│   │   └── get_custom_schema.sql
│   ├── tests/                # Custom tests
│   ├── dbt_project.yml       # dbt configuration
│   ├── profiles.yml          # Connection profiles
│   └── packages.yml          # dbt_utils dependency
│
├── metabase/           # Metabase configuration
│   ├──└── scripts/        # Setup scripts
│   └──init-db.sql         # PostgreSQL initialization
│
├── scripts/            # Python data fetching scripts
│   ├── fetch_wb_api.py          # World Bank API
│   ├── fetch_hdr_api.py         # UNDP HDR API
│   ├── fetch_imf_api.py         # IMF API
│   └── load_country_metadata.py # Country metadata loader
│
└── soda/               # Soda data quality checks
    ├── checks_raw.yml           # Raw layer validations
    ├── checks_marts.yml         # Marts layer validations
    ├── configuration_raw.yml    # Raw schema config
    └── configuration_marts.yml  # Marts schema config
```

---

## 🔧 Troubleshooting
### Common Issues

#### Port Conflicts
**Problem**: Port 8080 or 3000 already in use  
**Solution**: Modify port mappings in `docker-compose.yaml`
```yaml
services:
  airflow-webserver:
    ports:
      - "8081:8080"  # Change host port to 8081
  metabase:
    ports:
      - "3001:3000"  # Change host port to 3001
```

#### Permission Issues (Linux/Mac)
**Problem**: Permission denied errors  
**Solution**: Set proper AIRFLOW_UID in `.env`
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker-compose down -v
docker-compose up -d
```

#### Memory Issues
**Problem**: Services crashing or slow performance  
**Solution**: Increase Docker Desktop memory allocation
1. Docker Desktop → Settings → Resources
2. Increase Memory to at least 4GB (recommended: 6GB)
3. Click "Apply & Restart"

#### DAG Import Errors
**Problem**: DAGs not appearing in Airflow UI  
**Solution**: Check scheduler logs for Python errors
```powershell
docker-compose logs airflow-scheduler | Select-String -Pattern "ERROR"
docker-compose exec airflow-webserver bash
cd /opt/airflow/dags
python seed_data_dag.py  # Test import
```

#### DuckDB Connection Issues
**Problem**: "Database is locked" or connection errors  
**Solution**: Ensure only one process accesses DuckDB at a time
```powershell
# Stop all services
docker-compose down

# Remove lock files if they exist
Remove-Item data/warehouse.duckdb.wal -ErrorAction SilentlyContinue

# Restart services
docker-compose up -d
```

#### HDR API Key Issues
**Problem**: HDR DAG fails with authentication error  
**Solution**: Verify API key in `.env` file
```powershell
# Check if HDR_API_KEY is set
Get-Content .env | Select-String "HDR_API_KEY"

# Test API key manually
docker-compose exec airflow-webserver python -c "import os; print(os.getenv('HDR_API_KEY'))"
```

#### Metabase Connection Issues
**Problem**: Can't connect Metabase to DuckDB  
**Solution**: Use correct file path and permissions
- Database Type: DuckDB
- File Path: `/metabase-data/warehouse.duckdb`
- Read-only mode: Disabled
- Wait for first DAG run to complete (creates database file)

---

## 📈 Performance Optimization

### Current Performance Benchmarks
| Operation | Duration | Notes |
|-----------|----------|-------|
| **Load seed data** | ~30 seconds | One-time operation |
| **Fetch World Bank data** | 2-3 minutes | 28 indicators × 195 countries × 26 years |
| **Fetch HDR data** | 2-3 minutes | 23 indicators × 190 countries × 26 years |
| **Fetch IMF data** | 2-3 minutes | 5 indicators × 190 countries × 26 years |
| **Parallel fetch (WB+HDR+IMF)** | 3-4 minutes | ⭐ Longest of the 3 APIs |
| **dbt transformations** | 1-2 minutes | 4 staging views + 3 marts tables |
| **Soda quality checks** | 30 seconds | 2 layers of validation |
| **Full pipeline (sequential)** | ~10-15 minutes | Old method: DAGs run one after another |
| **Full pipeline (parallel)** | ~7-8 minutes | ⭐ NEW: Using `data_ingestion_pipeline` DAG |

### Scaling Triggers
| Metric | Current | Threshold | Action Required |
|--------|---------|-----------|-----------------|
| **Data Volume** | ~340K rows | >10M rows | Partition by year, incremental loading |
| **API Latency** | <5 min | >30 min | Implement caching, parallel fetching |
| **Transform Time** | <2 min | >10 min | Parallel dbt runs, incremental models |
| **Query Time** | <2 sec | >10 sec | Add indexes, materialized views |
| **Concurrent Users** | <5 | >20 | Migrate to PostgreSQL/Snowflake |

### Optimization Tips
1. ⭐ **Use `data_ingestion_pipeline` DAG**: Automatically runs WB, HDR, and IMF fetching in parallel (saves 4-7 minutes!)
2. **Incremental dbt Models**: For production, consider incremental refresh for recent years only
3. **DuckDB Settings**: Increase `memory_limit` and `threads` in connection string
4. **Docker Resources**: Allocate more RAM (6-8GB) for larger datasets

---

## 🧪 Testing

### Data Quality Checks
- **Pydantic Validation**: API response structure validation
- **dbt Tests**: Uniqueness, not null, relationships, custom tests
- **Soda Checks**: Row counts, duplicates, NULL thresholds, value ranges

### Running Tests Manually

```powershell
# dbt tests
docker-compose exec airflow-webserver bash
cd /opt/airflow/dbt
dbt test --profiles-dir .

# Soda checks
cd /opt/airflow/soda
soda scan -d global_macro_warehouse -c configuration_marts.yml checks_marts.yml
```

### Test Coverage
- ✅ **Raw layer**: Schema validation, NULL checks, row count > 0
- ✅ **Staging layer**: Duplicate detection, year range validation
- ✅ **Marts layer**: Primary key uniqueness, referential integrity, value ranges

---

## 📚 Documentation

- **[Architecture Diagrams](docs/ARCHITECTURE_DIAGRAM.md)** - System architecture, data flow, technology stack
- **[Project Audit Report](docs/PROJECT_AUDIT_REPORT.md)** - Comprehensive project health analysis
- **[Deployment Guide](docs/DEPLOYMENT.md)** - Production deployment instructions
- **[Cleanup Instructions](docs/CLEANUP.md)** - Environment cleanup and reset procedures

---

## 🤝 Contributing

### Development Workflow
1. Create feature branch from `main`
2. Make changes in local Docker environment
3. Test all affected DAGs
4. Run dbt tests and Soda checks
5. Update documentation if needed
6. Submit pull request

---

## 📝 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 🙏 Acknowledgments

**Data Sources**:
- [World Bank Open Data](https://data.worldbank.org/) - Economic and development indicators
- [UNDP Human Development Reports](https://hdr.undp.org/) - Human development indices
- [International Monetary Fund](https://www.imf.org/en/Data) - Macroeconomic and financial data

**Technologies**:
- [Apache Airflow](https://airflow.apache.org/) - Workflow orchestration
- [dbt](https://www.getdbt.com/) - Data transformation
- [DuckDB](https://duckdb.org/) - In-process OLAP database
- [Soda Core](https://www.soda.io/) - Data quality monitoring
- [Metabase](https://www.metabase.com/) - Business intelligence

---

## 📊 Project Status

**Current Version**: 1.0.0  
**Status**: Production Ready ✅  
**Health Score**: 95/100 🌟  
**Last Updated**: February 2, 2026

**Recent Updates**:
- ✅ Complete architecture documentation with Mermaid diagrams
- ✅ Comprehensive README with examples and troubleshooting
- ✅ Full data quality framework (3 layers)
- ✅ Docker Compose setup with 7 services
- ✅ Metabase integration for BI

**Next Steps**:
- 📊 Create sample Metabase dashboards
- 🔄 Implement incremental loading for recent years
- 📈 Performance monitoring with Prometheus/Grafana

---

**For questions or issues**, please open an issue on the repository or contact the maintainers.

**Happy analyzing! 📊🌍**
