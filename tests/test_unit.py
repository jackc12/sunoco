"""
Unit Tests for EIA Data Pipeline

Tests core transformation logic using in-memory mock data.
NO live API calls in unit tests.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import sys
from pathlib import Path

# Add src to path so we can import the application modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from gold.aggregate import GoldAggregation
from silver.transform import SilverTransformation


class TestGoldAggregation:
    """Test GoldAggregation class methods"""

    def test_calculate_annual_averages(self):
        """Test that calculate_annual_averages correctly averages monthly data"""
        # Create mock monthly data for 2 years
        mock_data = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=24, freq="MS"),
            "component": ["Production"] * 24,
            "value_mbblpd": [100.0] * 12 + [200.0] * 12,  # 100 for 2020, 200 for 2021
            "year": [2020] * 12 + [2021] * 12,
            "month": list(range(1, 13)) * 2
        })

        # Create instance and test the actual method
        aggregation = GoldAggregation.__new__(GoldAggregation)  # Don't call __init__ (avoids file checks)
        annual_avg = aggregation.calculate_annual_averages(mock_data)

        # Assertions
        assert len(annual_avg) == 2, "Should have 2 years of data"
        assert "annual_avg_mbblpd" in annual_avg.columns
        assert annual_avg[annual_avg["year"] == 2020]["annual_avg_mbblpd"].values[0] == 100.0
        assert annual_avg[annual_avg["year"] == 2021]["annual_avg_mbblpd"].values[0] == 200.0

    def test_calculate_annual_averages_partial_year(self):
        """Test aggregation with partial year data"""
        # Create mock data with only 6 months
        mock_data = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=6, freq="MS"),
            "component": ["Production"] * 6,
            "value_mbblpd": [100.0, 100.0, 100.0, 200.0, 200.0, 200.0],
            "year": [2020] * 6,
            "month": list(range(1, 7))
        })

        aggregation = GoldAggregation.__new__(GoldAggregation)
        annual_avg = aggregation.calculate_annual_averages(mock_data)

        # Should average to 150
        assert len(annual_avg) == 1
        assert annual_avg["annual_avg_mbblpd"].values[0] == 150.0


    def test_pivot_to_wide_format(self):
        """Test that pivot_to_wide_format creates one column per component"""
        # Create mock long-format data
        mock_data = pd.DataFrame({
            "year": [2020, 2020, 2020, 2021, 2021, 2021],
            "component": ["Production", "Imports", "Exports"] * 2,
            "annual_avg_mbblpd": [100.0, 50.0, 30.0, 110.0, 55.0, 35.0]
        })

        # Test the actual method
        aggregation = GoldAggregation.__new__(GoldAggregation)
        wide_df = aggregation.pivot_to_wide_format(mock_data)

        # Assertions
        assert len(wide_df) == 2, "Should have 2 rows (one per year)"
        assert "Production" in wide_df.columns
        assert "Imports" in wide_df.columns
        assert "Exports" in wide_df.columns
        assert wide_df[wide_df["year"] == 2020]["Production"].values[0] == 100.0

    def test_pivot_handles_missing_components(self):
        """Test that pivot handles missing data gracefully"""
        # Create mock data with missing component for one year
        mock_data = pd.DataFrame({
            "year": [2020, 2020, 2021],
            "component": ["Production", "Imports", "Production"],
            "annual_avg_mbblpd": [100.0, 50.0, 110.0]
        })

        # Test the actual method
        aggregation = GoldAggregation.__new__(GoldAggregation)
        wide_df = aggregation.pivot_to_wide_format(mock_data)

        # Assertions
        assert len(wide_df) == 2
        assert pd.isna(wide_df[wide_df["year"] == 2021]["Imports"].values[0])


class TestSilverTransformation:
    """Test SilverTransformation class methods"""

    def test_parse_series_data(self):
        """Test that parse_series_data correctly extracts data from API response"""
        # Create mock API response matching EIA structure
        # Using a real series_id from the config
        series_id = "MDIRPP32"  # Maps to "Production"

        mock_api_response = {
            "response": {
                "data": [
                    {"period": "2020-01", "value": "100.5", "series": series_id},
                    {"period": "2020-02", "value": "200.3", "series": series_id},
                    {"period": "2020-03", "value": "150.0", "series": series_id}
                ]
            }
        }

        # Test the actual method
        transformation = SilverTransformation.__new__(SilverTransformation)
        df = transformation.parse_series_data(series_id, mock_api_response)

        # Assertions
        assert len(df) == 3
        assert "period" in df.columns
        assert "value" in df.columns
        assert "series_id" in df.columns
        assert "component" in df.columns
        assert df["series_id"].iloc[0] == series_id
        assert df["component"].iloc[0] == "Production"  # Check it mapped correctly

    def test_parse_series_data_empty_response(self):
        """Test that parse_series_data handles empty API responses"""
        mock_api_response = {
            "response": {
                "data": []
            }
        }

        series_id = "MDIRPP32"  # Use a valid series_id

        transformation = SilverTransformation.__new__(SilverTransformation)
        df = transformation.parse_series_data(series_id, mock_api_response)

        # Should return empty DataFrame
        assert len(df) == 0

    def test_clean_and_normalize(self):
        """Test that clean_and_normalize properly cleans data"""
        # Create mock raw data
        mock_data = pd.DataFrame({
            "period": ["2020-01", "2020-02", "2020-03", "2020-04"],
            "series_id": ["PET.M_EPPO_FPF_R30_MBBLPD.M"] * 4,
            "component": ["Production"] * 4,
            "value": ["100.5", "200.3", "invalid", "150.0"]  # One invalid value
        })

        # Test the actual method
        transformation = SilverTransformation.__new__(SilverTransformation)
        clean_df = transformation.clean_and_normalize(mock_data)

        # Assertions
        assert "date" in clean_df.columns
        assert "value_mbblpd" in clean_df.columns
        assert "year" in clean_df.columns
        assert "month" in clean_df.columns
        assert "category" in clean_df.columns
        assert clean_df["date"].dtype == "datetime64[ns]"
        assert clean_df["value_mbblpd"].dtype in [np.float64, float]

        # Should remove the invalid row
        assert len(clean_df) == 3

        # Check date parsing
        assert clean_df["year"].iloc[0] == 2020
        assert clean_df["month"].iloc[0] == 1


class TestBalanceCalculation:
    """Test supply/disposition balance calculation using GoldAggregation.add_balance_check"""

    def test_add_balance_check_perfect_match(self):
        """Test add_balance_check when supply equals disposition"""
        mock_data = pd.DataFrame({
            "year": [2020],
            "Production": [100.0],
            "Imports": [50.0],
            "Net_Receipts": [30.0],
            "Exports": [20.0],
            "Product_Supplied": [150.0],
            "Stock_Change": [10.0]
        })

        # Test the actual method
        aggregation = GoldAggregation.__new__(GoldAggregation)
        result_df = aggregation.add_balance_check(mock_data)

        # Assertions
        assert "Total_Supply" in result_df.columns
        assert "Total_Disposition" in result_df.columns
        assert "Balance_Difference" in result_df.columns
        assert "Balance_Pct_Diff" in result_df.columns
        assert result_df["Total_Supply"].values[0] == 180.0
        assert result_df["Total_Disposition"].values[0] == 180.0
        assert result_df["Balance_Difference"].values[0] == 0.0
        assert result_df["Balance_Pct_Diff"].values[0] == 0.0

    def test_add_balance_check_with_difference(self):
        """Test add_balance_check with supply/disposition mismatch"""
        mock_data = pd.DataFrame({
            "year": [2020],
            "Production": [100.0],
            "Imports": [50.0],
            "Net_Receipts": [30.0],
            "Exports": [20.0],
            "Product_Supplied": [140.0],  # Less than supply
            "Stock_Change": [10.0]
        })

        # Test the actual method
        aggregation = GoldAggregation.__new__(GoldAggregation)
        result_df = aggregation.add_balance_check(mock_data)

        # Assertions
        assert result_df["Total_Supply"].values[0] == 180.0
        assert result_df["Total_Disposition"].values[0] == 170.0
        assert result_df["Balance_Difference"].values[0] == 10.0
        # 10 / 180 * 100 = 5.56%
        assert abs(result_df["Balance_Pct_Diff"].values[0] - 5.56) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
