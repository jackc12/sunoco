#!/usr/bin/env python3
"""
Main Pipeline Runner

Orchestrates the complete ETL pipeline:
1. Bronze: Raw data ingestion from EIA API
2. Silver: Data cleaning and normalization
3. Gold: Annual aggregation and analytics

Usage:
    python run_pipeline.py              # Run full pipeline
    python run_pipeline.py --bronze     # Run only bronze layer
    python run_pipeline.py --silver     # Run only silver layer
    python run_pipeline.py --gold       # Run only gold layer
"""

import os
import sys
import argparse
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv
load_dotenv(".env.sh")

# Ensure src is in path
sys.path.append(str(Path(__file__).parent / "src"))

from src.bronze.ingest import EIADataIngestion
from src.silver.transform import SilverTransformation
from src.gold.aggregate import GoldAggregation


def run_bronze():
    """Run bronze layer ingestion"""
    print("\n" + "=" * 70)
    print("STEP 1/3: BRONZE LAYER - Raw Data Ingestion")
    print("=" * 70)

    try:
        ingestion = EIADataIngestion()
        ingestion.run()
        print("\n✓ Bronze layer completed successfully\n")
        return True
    except Exception as e:
        print(f"\n✗ Bronze layer failed: {e}\n")
        return False


def run_silver():
    """Run silver layer transformation"""
    print("\n" + "=" * 70)
    print("STEP 2/3: SILVER LAYER - Data Cleaning & Normalization")
    print("=" * 70)

    try:
        transformation = SilverTransformation()
        transformation.run()
        print("\n✓ Silver layer completed successfully\n")
        return True
    except Exception as e:
        print(f"\n✗ Silver layer failed: {e}\n")
        return False


def run_gold():
    """Run gold layer aggregation"""
    print("\n" + "=" * 70)
    print("STEP 3/3: GOLD LAYER - Annual Aggregation & Analytics")
    print("=" * 70)

    try:
        aggregation = GoldAggregation()
        df = aggregation.run()

        # Display summary
        print("\n" + "=" * 70)
        print("PIPELINE SUMMARY")
        print("=" * 70)
        print(f"\nAnnual data generated for {len(df)} years")
        print(f"Date range: {df['year'].min()} - {df['year'].max()}")
        print(f"Components: {len([col for col in df.columns if col != 'year'])}")

        # Show sample of data
        print("\nSample of Gold Layer Data:")
        print(df.head())

        print("\n✓ Gold layer completed successfully\n")
        return True
    except Exception as e:
        print(f"\n✗ Gold layer failed: {e}\n")
        return False


def run_full_pipeline():
    """Run complete ETL pipeline"""
    print("\n" + "=" * 70)
    print("EIA DATA PIPELINE - FULL RUN")
    print("PADD 3 Distillate Fuel Oil Monthly Data")
    print("=" * 70)

    # Check API key
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("\n✗ Error: API_KEY not found in environment")
        print("Please set your EIA API key in .env.sh")
        return False

    # Run each layer
    success = True

    success = success and run_bronze()
    if not success:
        return False

    success = success and run_silver()
    if not success:
        return False

    success = success and run_gold()

    if success:
        print("\n" + "=" * 70)
        print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print("\nNext steps:")
        print("1. Run tests: pytest tests/ -v")
        print("2. View analysis: jupyter notebook notebooks/pipeline_documentation.ipynb")
        print("\nData outputs:")
        print("  - Bronze: data/bronze/eia_raw_responses.json")
        print("  - Silver: data/silver/distillate_monthly_clean.csv")
        print("  - Gold:   data/gold/distillate_annual_averages.csv")
        print("=" * 70)

    return success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Run EIA Data ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py              # Run full pipeline
  python run_pipeline.py --bronze     # Run only bronze layer
  python run_pipeline.py --silver     # Run only silver layer
  python run_pipeline.py --gold       # Run only gold layer
        """
    )

    parser.add_argument(
        "--bronze",
        action="store_true",
        help="Run only bronze layer (raw ingestion)"
    )
    parser.add_argument(
        "--silver",
        action="store_true",
        help="Run only silver layer (cleaning)"
    )
    parser.add_argument(
        "--gold",
        action="store_true",
        help="Run only gold layer (aggregation)"
    )

    args = parser.parse_args()

    # If no specific layer selected, run full pipeline
    if not (args.bronze or args.silver or args.gold):
        success = run_full_pipeline()
    else:
        success = True
        if args.bronze:
            success = success and run_bronze()
        if args.silver:
            success = success and run_silver()
        if args.gold:
            success = success and run_gold()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
