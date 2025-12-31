"""
Validation Tests for EIA Data Pipeline

Tests data quality and reasonableness using actual pipeline output.
Validates business rules and data integrity.
"""

import pytest
import pandas as pd
from pathlib import Path

# Add parent directory to path
import sys
sys.path.append(str(Path(__file__).parent.parent))

from src.config import (
    SILVER_DIR,
    GOLD_DIR,
    SILVER_CLEAN_FILE,
    GOLD_ANNUAL_FILE,
    SUPPLY_COMPONENTS,
    DISPOSITION_COMPONENTS
)


class TestSilverDataQuality:
    """Validation tests for Silver layer data"""

    @pytest.fixture
    def silver_data(self):
        """Load silver layer data if it exists"""
        silver_path = SILVER_DIR / SILVER_CLEAN_FILE
        if not silver_path.exists():
            pytest.skip(f"Silver data not found at {silver_path}. Run pipeline first.")
        return pd.read_csv(silver_path, parse_dates=["date"])

    def test_no_negative_values(self, silver_data):
        """Validate that negative MBBL/D values are only in components where expected"""
        # Negative values are valid for Net_Receipts (net outflow) and Stock_Change (inventory draw)
        # But not valid for Production, Imports, Exports, Product_Supplied
        components_no_negatives = ["Production", "Imports", "Exports", "Product_Supplied"]

        for component in components_no_negatives:
            comp_data = silver_data[silver_data["component"] == component]
            negative_values = comp_data[comp_data["value_mbblpd"] < 0]

            assert len(negative_values) == 0, (
                f"Component '{component}' has {len(negative_values)} negative values, "
                f"which is not valid for this metric."
            )

    def test_expected_date_range(self, silver_data):
        """Validate that data covers expected date range"""
        min_date = silver_data["date"].min()
        max_date = silver_data["date"].max()

        # Should start from 2015 or later
        assert min_date.year >= 2015, (
            f"Data starts from {min_date}, expected 2015 or later"
        )

        # Should have recent data (within last few years)
        assert max_date.year >= 2023, (
            f"Data ends at {max_date}, expected more recent data"
        )

    def test_expected_number_of_components(self, silver_data):
        """Validate that all expected components are present"""
        components = silver_data["component"].unique()

        # We expect at least 5 components (can be more if EIA adds more)
        assert len(components) >= 5, (
            f"Expected at least 5 components, found {len(components)}: {components}"
        )

    def test_monthly_frequency(self, silver_data):
        """Validate that most data is at monthly frequency"""
        # For each component, check that MOST consecutive records are monthly
        # Some components may have significant gaps (e.g., Imports has ~58% monthly)
        for component in silver_data["component"].unique():
            comp_data = silver_data[silver_data["component"] == component].copy()
            comp_data = comp_data.sort_values("date")

            if len(comp_data) > 5:
                # Check intervals between consecutive records
                comp_data['date_diff'] = comp_data['date'].diff().dt.days

                # At least 50% of intervals should be monthly (28-31 days)
                # This is relaxed to accommodate series like Imports with historical gaps
                monthly_intervals = comp_data[(comp_data['date_diff'] >= 28) &
                                             (comp_data['date_diff'] <= 31)]
                monthly_pct = len(monthly_intervals) / (len(comp_data) - 1)

                assert monthly_pct >= 0.5, (
                    f"Component {component}: Only {monthly_pct*100:.1f}% of intervals "
                    f"are monthly. Expected at least 50%."
                )

    def test_no_missing_values_in_key_columns(self, silver_data):
        """Validate that key columns have no missing values"""
        required_columns = ["date", "component", "value_mbblpd", "year"]

        for col in required_columns:
            missing_count = silver_data[col].isna().sum()
            assert missing_count == 0, (
                f"Column '{col}' has {missing_count} missing values"
            )


class TestGoldDataQuality:
    """Validation tests for Gold layer data"""

    @pytest.fixture
    def gold_data(self):
        """Load gold layer data if it exists"""
        gold_path = GOLD_DIR / GOLD_ANNUAL_FILE
        if not gold_path.exists():
            pytest.skip(f"Gold data not found at {gold_path}. Run pipeline first.")
        return pd.read_csv(gold_path)

    def test_non_empty_annual_averages(self, gold_data):
        """Validate that annual averages are non-empty"""
        assert len(gold_data) > 0, "Gold layer has no data"

        # Should have at least 9 years (2015-2024)
        assert len(gold_data) >= 9, (
            f"Expected at least 9 years of data, found {len(gold_data)}"
        )

    def test_all_components_present(self, gold_data):
        """Validate that all expected component columns exist"""
        # Check for supply components
        for component in SUPPLY_COMPONENTS:
            assert component in gold_data.columns, (
                f"Supply component '{component}' missing from gold data"
            )

        # Check for disposition components
        for component in DISPOSITION_COMPONENTS:
            assert component in gold_data.columns, (
                f"Disposition component '{component}' missing from gold data"
            )

    def test_no_negative_averages(self, gold_data):
        """Validate that annual averages are non-negative where expected"""
        # Negative values are valid for Net_Receipts (net outflow) and Stock_Change (inventory draw)
        # But not valid for Production, Imports, Exports, Product_Supplied
        components_no_negatives = ["Production", "Imports", "Exports", "Product_Supplied"]

        for col in components_no_negatives:
            if col in gold_data.columns:
                negative_values = gold_data[gold_data[col] < 0]
                assert len(negative_values) == 0, (
                    f"Column '{col}' has {len(negative_values)} negative annual averages, "
                    f"which is not valid for this metric."
                )

    def test_balance_check(self, gold_data):
        """Validate supply/disposition balance with tolerance"""
        # Check if balance columns exist
        if "Total_Supply" not in gold_data.columns or "Total_Disposition" not in gold_data.columns:
            pytest.skip("Balance columns not found in gold data")

        # Calculate balance difference percentage
        balance_diff = gold_data["Balance_Difference"].abs()
        balance_pct = (balance_diff / gold_data["Total_Supply"] * 100)

        # Allow up to 5% difference due to rounding, timing, or inventory adjustments
        tolerance = 5.0  # 5%

        out_of_balance = gold_data[balance_pct > tolerance]

        # Most years should balance within tolerance
        balance_rate = 1 - (len(out_of_balance) / len(gold_data))
        assert balance_rate >= 0.7, (
            f"Only {balance_rate*100:.1f}% of years balance within {tolerance}% tolerance. "
            f"Expected at least 70% to balance."
        )

    def test_reasonable_value_ranges(self, gold_data):
        """Validate that values are in reasonable ranges (not outliers)"""
        # For PADD 3 distillate, typical values are in hundreds to thousands MBBL/D
        # Check production is reasonable (should be > 0 and < 10,000 MBBL/D for PADD 3)
        if "Production" in gold_data.columns:
            prod_values = gold_data["Production"]
            assert prod_values.min() > 0, "Production should be positive"
            assert prod_values.max() < 10000, (
                f"Production max {prod_values.max()} seems unreasonably high"
            )

    def test_year_column_present_and_sequential(self, gold_data):
        """Validate that year column is present and years are sequential"""
        assert "year" in gold_data.columns, "Year column missing from gold data"

        years = sorted(gold_data["year"].unique())

        # Check years are sequential (or nearly sequential)
        for i in range(len(years) - 1):
            year_diff = years[i + 1] - years[i]
            assert year_diff <= 2, (
                f"Gap in years: {years[i]} to {years[i+1]}"
            )


class TestDataCompleteness:
    """Tests for overall data completeness"""

    @pytest.fixture
    def silver_data(self):
        """Load silver layer data if it exists"""
        silver_path = SILVER_DIR / SILVER_CLEAN_FILE
        if not silver_path.exists():
            pytest.skip(f"Silver data not found at {silver_path}. Run pipeline first.")
        return pd.read_csv(silver_path, parse_dates=["date"])

    def test_expected_number_of_records(self, silver_data):
        """Validate expected number of records in silver layer"""
        # We have 6 components, monthly data from 2015 to present (~10 years = 120 months)
        # Expected: 6 components * 120 months = ~720 records minimum

        expected_min_records = 6 * 100  # Conservative estimate: 6 components * 100 months

        assert len(silver_data) >= expected_min_records, (
            f"Expected at least {expected_min_records} records, found {len(silver_data)}"
        )

    def test_all_components_have_similar_record_counts(self, silver_data):
        """Validate that all components have reasonable number of records"""
        component_counts = silver_data.groupby("component").size()

        # Each component should have at least 50% of the max count
        # (Some EIA series have gaps in historical data)
        max_count = component_counts.max()
        min_count = component_counts.min()

        ratio = min_count / max_count

        assert ratio >= 0.5, (
            f"Component record counts vary too much. "
            f"Min: {min_count}, Max: {max_count}, Ratio: {ratio:.2f}. "
            f"Each component should have at least 50% of records compared to the most complete series."
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
