"""
Bronze Layer: Raw data ingestion from EIA API

This module fetches raw monthly data from the EIA API for all PADD 3
Distillate Fuel Oil series starting from January 2015.

No transformations are applied - data is saved as-is from the API.
"""

import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import time

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    EIA_API_KEY,
    EIA_API_BASE_URL,
    SERIES_MAPPING,
    START_DATE,
    BRONZE_DIR,
    BRONZE_RAW_FILE
)


class EIADataIngestion:
    """Handles raw data ingestion from EIA API"""

    def __init__(self, api_key: str = None):
        """
        Initialize the EIA data ingestion client

        Args:
            api_key: EIA API key. If None, uses API_KEY from environment
        """
        self.api_key = api_key or EIA_API_KEY
        if not self.api_key:
            raise ValueError("EIA API key not found. Set API_KEY environment variable.")

        self.base_url = EIA_API_BASE_URL
        self.series_ids = list(SERIES_MAPPING.keys())

    def fetch_series_data(self, series_id: str, start_date: str = START_DATE) -> Dict:
        """
        Fetch data for a single EIA series

        Args:
            series_id: EIA series identifier
            start_date: Start date in YYYY-MM format

        Returns:
            Raw API response as dictionary
        """
        # EIA API v2 endpoint structure
        url = f"{self.base_url}/petroleum/sum/snd/data/"

        params = {
            "api_key": self.api_key,
            "frequency": "monthly",
            "data[0]": "value",
            "facets[series][]": series_id,
            "start": start_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": 0,
            "length": 5000
        }

        print(f"Fetching data for series: {series_id} ({SERIES_MAPPING[series_id]})")

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Add metadata
            data["_metadata"] = {
                "series_id": series_id,
                "component_name": SERIES_MAPPING[series_id],
                "fetch_timestamp": datetime.now().isoformat(),
                "start_date": start_date
            }

            # Log data retrieved
            if "response" in data and "data" in data["response"]:
                record_count = len(data["response"]["data"])
                print(f"  Retrieved {record_count} records for {series_id}")

            return data

        except requests.exceptions.RequestException as e:
            print(f"Error fetching series {series_id}: {e}")
            raise

    def fetch_all_series(self) -> Dict[str, Dict]:
        """
        Fetch data for all configured EIA series

        Returns:
            Dictionary mapping series_id to raw API response
        """
        all_data = {}

        for series_id in self.series_ids:
            all_data[series_id] = self.fetch_series_data(series_id)
            # Be nice to the API
            time.sleep(0.5)

        return all_data

    def save_raw_data(self, data: Dict[str, Dict], output_path: Path = None) -> Path:
        """
        Save raw API responses to JSON file

        Args:
            data: Dictionary of raw API responses
            output_path: Path to save JSON file. If None, uses default

        Returns:
            Path to saved file
        """
        if output_path is None:
            output_path = BRONZE_DIR / BRONZE_RAW_FILE

        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save with pretty printing for readability
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"\nRaw data saved to: {output_path}")
        return output_path

    def run(self) -> Path:
        """
        Execute the complete bronze layer ingestion

        Returns:
            Path to saved raw data file
        """
        print("=" * 60)
        print("BRONZE LAYER: Raw Data Ingestion")
        print("=" * 60)
        print(f"Fetching {len(self.series_ids)} series from EIA API...")
        print()

        # Fetch all data
        raw_data = self.fetch_all_series()

        # Save to bronze layer
        output_path = self.save_raw_data(raw_data)

        print("\nBronze layer ingestion complete!")
        print("=" * 60)

        return output_path


def main():
    """Run bronze layer ingestion as standalone script"""
    ingestion = EIADataIngestion()
    ingestion.run()


if __name__ == "__main__":
    main()
