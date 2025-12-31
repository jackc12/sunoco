"""
Configuration file for EIA data pipeline
Contains EIA series IDs and API configuration
"""

import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# EIA API Configuration
EIA_API_BASE_URL = "https://api.eia.gov/v2"
EIA_API_KEY = os.getenv("API_KEY", "")

# PADD 3 Distillate Fuel Oil Series IDs
# Mapping from EIA Series ID to human-readable component name
SERIES_MAPPING = {
    # Supply Components
    "MDIRPP32": "Production",
    "MDIIMP32": "Imports",
    "MDINRP32": "Net_Receipts",

    # Disposition Components
    "MDISCP32": "Stock_Change",
    "MDIEXP32": "Exports",
    "MDIUPP32": "Product_Supplied",
}

# Supply vs Disposition classification
SUPPLY_COMPONENTS = ["Production", "Imports", "Net_Receipts"]
DISPOSITION_COMPONENTS = ["Exports", "Product_Supplied", "Stock_Change"]

# Date range for data ingestion
START_DATE = "2015-01"  # January 2015
# END_DATE will be determined dynamically (most recent available)

# File names
BRONZE_RAW_FILE = "eia_raw_responses.json"
SILVER_CLEAN_FILE = "distillate_monthly_clean.csv"
GOLD_ANNUAL_FILE = "distillate_annual_averages.csv"
