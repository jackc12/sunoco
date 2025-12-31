"""
Silver Layer: Data cleaning and normalization

This module loads raw Bronze data and creates a clean, normalized dataset:
- Parses timestamps
- Normalizes column names to human-readable format
- Ensures numeric types
- Produces long/tidy format (one row per month per series)
- Units remain as MBBL/D (thousand barrels per day)
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    BRONZE_DIR,
    SILVER_DIR,
    BRONZE_RAW_FILE,
    SILVER_CLEAN_FILE,
    SERIES_MAPPING,
    SUPPLY_COMPONENTS,
    DISPOSITION_COMPONENTS
)


class SilverTransformation:
    """Handles data cleaning and normalization"""

    def __init__(self, bronze_path: Path = None):
        """
        Initialize the silver layer transformation

        Args:
            bronze_path: Path to bronze raw data file. If None, uses default
        """
        self.bronze_path = bronze_path or (BRONZE_DIR / BRONZE_RAW_FILE)

        if not self.bronze_path.exists():
            raise FileNotFoundError(
                f"Bronze data not found at {self.bronze_path}. "
                "Run bronze layer ingestion first."
            )

    def load_raw_data(self) -> Dict:
        """
        Load raw data from bronze layer

        Returns:
            Dictionary of raw API responses
        """
        with open(self.bronze_path, 'r') as f:
            raw_data = json.load(f)

        print(f"Loaded raw data from: {self.bronze_path}")
        return raw_data

    def parse_series_data(self, series_id: str, api_response: Dict) -> pd.DataFrame:
        """
        Parse a single series API response into a DataFrame

        Args:
            series_id: EIA series identifier
            api_response: Raw API response dictionary

        Returns:
            DataFrame with columns: period, series_id, component, value
        """
        # Extract the data records
        if "response" not in api_response or "data" not in api_response["response"]:
            print(f"Warning: No data found for series {series_id}")
            return pd.DataFrame()

        records = api_response["response"]["data"]

        if not records:
            print(f"Warning: Empty data for series {series_id}")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Add series_id and component name
        df["series_id"] = series_id
        df["component"] = SERIES_MAPPING[series_id]

        return df

    def clean_and_normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize the data

        Args:
            df: Raw DataFrame from all series combined

        Returns:
            Cleaned and normalized DataFrame
        """
        print("Cleaning and normalizing data...")

        # Select and rename columns
        df = df[["period", "series_id", "component", "value"]].copy()

        # Rename columns for clarity
        df.columns = ["date", "series_id", "component", "value_mbblpd"]

        # Parse dates (period is in YYYY-MM format)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m")

        # Ensure numeric type for values
        df["value_mbblpd"] = pd.to_numeric(df["value_mbblpd"], errors="coerce")

        # Add year and month columns for convenience
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

        # Add component classification (Supply vs Disposition)
        df["category"] = df["component"].apply(
            lambda x: "Supply" if x in SUPPLY_COMPONENTS
            else "Disposition" if x in DISPOSITION_COMPONENTS
            else "Other"
        )

        # Sort by date and component
        df = df.sort_values(["date", "component"]).reset_index(drop=True)

        # Remove any rows with missing values
        initial_rows = len(df)
        df = df.dropna(subset=["value_mbblpd"])
        if len(df) < initial_rows:
            print(f"  Removed {initial_rows - len(df)} rows with missing values")

        print(f"  Final dataset: {len(df)} rows, {df['component'].nunique()} components")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")

        return df

    def save_clean_data(self, df: pd.DataFrame, output_path: Path = None) -> Path:
        """
        Save cleaned data to CSV

        Args:
            df: Cleaned DataFrame
            output_path: Path to save CSV file. If None, uses default

        Returns:
            Path to saved file
        """
        if output_path is None:
            output_path = SILVER_DIR / SILVER_CLEAN_FILE

        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save to CSV
        df.to_csv(output_path, index=False)

        print(f"Clean data saved to: {output_path}")
        return output_path

    def run(self) -> pd.DataFrame:
        """
        Execute the complete silver layer transformation

        Returns:
            Cleaned DataFrame
        """
        print("=" * 60)
        print("SILVER LAYER: Data Cleaning & Normalization")
        print("=" * 60)

        # Load raw data
        raw_data = self.load_raw_data()

        # Parse all series
        print(f"\nParsing {len(raw_data)} series...")
        dfs = []
        for series_id, api_response in raw_data.items():
            df = self.parse_series_data(series_id, api_response)
            if not df.empty:
                dfs.append(df)

        # Combine all series
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"Combined {len(dfs)} series into {len(combined_df)} rows")

        # Clean and normalize
        clean_df = self.clean_and_normalize(combined_df)

        # Save to silver layer
        self.save_clean_data(clean_df)

        print("\nSilver layer transformation complete!")
        print("=" * 60)

        return clean_df


def main():
    """Run silver layer transformation as standalone script"""
    transformation = SilverTransformation()
    transformation.run()


if __name__ == "__main__":
    main()
