# EIA Data Pipeline - PADD 3 Distillate Fuel Oil Analysis

**Supply & Trading Analytics | Data Engineering Simulation**

A production-ready ETL pipeline for extracting, transforming, and analyzing Energy Information Administration (EIA) data on PADD 3 (Gulf Coast) Distillate Fuel Oil supply and demand.

---

## Overview

This pipeline supports Sunoco's Supply & Trading team by providing clean, reliable EIA data for market analysis and decision-making. It processes monthly data from January 2015 to present across 6 key supply and disposition components.

### Key Features

- **Medallion Architecture**: Bronze (raw) → Silver (clean) → Gold (analytics)
- **Data Quality**: Comprehensive validation and balance checks
- **Testable**: Unit tests and integration tests
- **Production-Ready**: Error handling, logging, and modular design
- **Analytics**: Jupyter notebook with market insights and visualizations

---

## Quick Start

### 1. Prerequisites

- Python 3.8+
- EIA API Key (free from https://www.eia.gov/opendata/register.php)

### 2. Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd sunoco-sim

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up API key
echo "export API_KEY=your_eia_api_key_here" > .env.sh
```

### 3. Run the Pipeline

```bash
# Run complete pipeline (Bronze → Silver → Gold)
python run_pipeline.py

# Or run individual layers
python run_pipeline.py --bronze  # Raw data ingestion
python run_pipeline.py --silver  # Data cleaning
python run_pipeline.py --gold    # Annual aggregation
```

### 4. View Results

```bash
# Run tests
pytest tests/ -v

# View analysis notebook
jupyter notebook notebooks/pipeline_documentation.ipynb
```

---

## Project Structure

```
sunoco-sim/
├── src/                          # Source code
│   ├── bronze/                   # Raw data ingestion layer
│   │   ├── __init__.py
│   │   └── ingest.py            # EIA API client
│   ├── silver/                   # Data cleaning layer
│   │   ├── __init__.py
│   │   └── transform.py         # Normalization logic
│   ├── gold/                     # Analytics layer
│   │   ├── __init__.py
│   │   └── aggregate.py         # Annual aggregation
│   └── config.py                # Configuration & constants
│
├── tests/                        # Test suite
│   ├── __init__.py
│   ├── test_unit.py             # Unit tests (mock data)
│   └── test_validation.py       # Data quality tests
│
├── data/                         # Data storage (gitignored)
│   ├── bronze/                  # Raw API responses (.json)
│   ├── silver/                  # Clean monthly data (.csv)
│   └── gold/                    # Annual aggregates (.csv)
│
├── notebooks/                    # Analysis notebooks
│   └── pipeline_documentation.ipynb  # Main deliverable
│
├── run_pipeline.py              # Pipeline orchestrator
├── requirements.txt             # Python dependencies
├── .env.sh                      # API key (gitignored)
├── .gitignore
└── README.md                    # This file
```

---

## Data Pipeline Layers

### Bronze Layer (Raw Ingestion)

**Purpose**: Extract raw data from EIA API and store as-is

**Process**:
- Fetches monthly data for 6 EIA series from Jan 2015 to present
- Saves complete API responses to JSON
- No transformations applied

**Output**: `data/bronze/eia_raw_responses.json`

**Run**: `python src/bronze/ingest.py`

### Silver Layer (Cleaning & Normalization)

**Purpose**: Transform raw data into clean, analysis-ready format

**Process**:
- Parses JSON responses into structured DataFrame
- Normalizes column names to human-readable format
- Ensures correct data types (dates, numerics)
- Produces long/tidy format (one row per month per series)
- Validates data quality (no negatives, no missing values)

**Output**: `data/silver/distillate_monthly_clean.csv`

**Run**: `python src/silver/transform.py`

### Gold Layer (Annual Aggregation)

**Purpose**: Create analytics-optimized annual averages

**Process**:
- Calculates annual averages from monthly data
- Pivots to wide format (one column per component)
- Adds supply/disposition balance checks
- Schema: Year | Production | Imports | Exports | ...

**Output**: `data/gold/distillate_annual_averages.csv`

**Run**: `python src/gold/aggregate.py`

---

## Data Mapping

### EIA Series to Component Names

| EIA Series ID | Component Name | Category | Description |
|---------------|----------------|----------|-------------|
| MDIRPP32 | Production | Supply | Refinery & Blender Net Production |
| MDIIMP32 | Imports | Supply | Imports from other countries |
| MDINRP32 | Net_Receipts | Supply | Net receipts from other PADDs |
| MDISCP32 | Stock_Change | Disposition | Change in inventory levels |
| MDIEXP32 | Exports | Disposition | Exports to other countries |
| MDIUPP32 | Product_Supplied | Disposition | Proxy for demand/consumption |

All values are in **MBBL/D** (thousand barrels per day).

### Supply & Disposition Balance

In petroleum accounting:

```
Supply = Production + Imports + Net_Receipts
Disposition = Exports + Product_Supplied + Stock_Change

Supply ≈ Disposition (within 5% tolerance)
```

---

## Testing

The pipeline includes comprehensive tests for data quality and transformation logic.

### Unit Tests

Test core transformation logic with mock data (no API calls):

```bash
pytest tests/test_unit.py -v
```

**Coverage**:
- Monthly to annual aggregation
- Long to wide format pivot
- Date parsing and type conversion
- Balance calculation logic

### Validation Tests

Test data quality using actual pipeline output:

```bash
pytest tests/test_validation.py -v
```

**Coverage**:
- No negative MBBL/D values
- Expected date range (2015+)
- Monthly frequency validation
- Supply/disposition balance within tolerance
- Reasonable value ranges
- Data completeness checks

### Run All Tests

```bash
# With verbose output
pytest tests/ -v

# With coverage report
pytest tests/ --cov=src --cov-report=html
```

---

## Jupyter Notebook

The main deliverable is a comprehensive Jupyter notebook that includes:

### Contents

1. **Pipeline Instructions**: How to run each layer
2. **Data Mapping**: EIA series IDs to component names
3. **Architecture & Design**: Folder structure and design decisions
4. **Market Analysis**:
   - Visualizations of supply/demand trends
   - Component-level trend analysis
   - Balance verification
   - Market hypothesis and insights
5. **Test Results**: Execution of test suite

### Running the Notebook

```bash
jupyter notebook notebooks/pipeline_documentation.ipynb
```

---

## Configuration

### API Key Setup

The pipeline requires an EIA API key. Two options:

**Option 1: Environment file (recommended)**
```bash
echo "export API_KEY=your_api_key_here" > .env.sh
source .env.sh
```

**Option 2: Environment variable**
```bash
export API_KEY=your_api_key_here
python run_pipeline.py
```

### Customization

Edit `src/config.py` to customize:
- Date range for ingestion
- Output file names
- Series mappings
- Data directories

---

## Design Decisions

### Why Medallion Architecture?

- **Bronze**: Preserves raw data for audit trail and reprocessing
- **Silver**: Single source of truth for clean data
- **Gold**: Optimized for specific analytics use cases

### Why These File Formats?

- **Bronze (JSON)**: Preserves API structure, human-readable
- **Silver (CSV)**: Widely compatible, easy to inspect
- **Gold (CSV)**: Excel-compatible for business users

### Why Separate Layers?

- Each layer can be re-run independently
- Easy to debug specific transformation steps
- Enables incremental updates in production
- Follows data engineering best practices

### Security

- API keys in `.env.sh` (gitignored)
- No hardcoded secrets
- Data directory gitignored (is reproducible)

---

## Production Deployment

### Automation

Schedule regular runs via cron or Apache Airflow:

```bash
# Weekly on Monday at 6 AM
0 6 * * 1 cd /path/to/sunoco-sim && source .env.sh && python run_pipeline.py
```

### Monitoring

Add alerting for:
- API failures
- Data quality validation failures
- Balance check failures
- Missing recent data

### Incremental Updates

For production, consider modifying bronze layer to:
- Only fetch data since last successful run
- Append to existing files rather than overwrite
- Track ingestion metadata (run time, record counts)

---

## Future Enhancements

1. **Expand Coverage**
   - Add other PADDs (1, 2, 4, 5)
   - Include other products (gasoline, jet fuel, crude)
   - Add weekly data for more timely insights

2. **Advanced Analytics**
   - Time series forecasting (ARIMA, Prophet)
   - Anomaly detection
   - Correlation with market prices

3. **Data Warehouse Integration**
   - Load gold data into Snowflake/BigQuery
   - Build Tableau/Power BI dashboards
   - Enable self-service analytics

4. **API Improvements**
   - Async API calls for better performance
   - Retry logic with exponential backoff
   - Rate limiting management

---

## Troubleshooting

### API Key Errors

```
ValueError: EIA API key not found
```

**Solution**: Set API_KEY in `.env.sh` and source it

### Import Errors

```
ModuleNotFoundError: No module named 'pandas'
```

**Solution**: Install dependencies with `pip install -r requirements.txt`

### Data Not Found

```
FileNotFoundError: Bronze data not found
```

**Solution**: Run bronze layer first: `python run_pipeline.py --bronze`

### Test Failures

If validation tests fail, check:
- Pipeline has been run successfully
- Data files exist in expected locations
- No API rate limiting issues
