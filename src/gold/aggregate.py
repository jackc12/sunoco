"""
Gold Layer: Annual aggregation and analytics

This module converts monthly data into annual averages in wide format:
- Aggregates monthly values to annual averages
- Pivots data to wide format (one column per component)
- Schema: Year | Production | Imports | ... | Exports | Product_Supplied
"""

import pandas as pd
from pathlib import Path
from typing import List

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    SILVER_DIR,
    GOLD_DIR,
    SILVER_CLEAN_FILE,
    GOLD_ANNUAL_FILE,
    SERIES_MAPPING,
    SUPPLY_COMPONENTS,
    DISPOSITION_COMPONENTS
)


class GoldAggregation:
    """Handles annual aggregation and analytics"""

    def __init__(self, silver_path: Path = None):
        """
        Initialize the gold layer aggregation

        Args:
            silver_path: Path to silver clean data file. If None, uses default
        """
        self.silver_path = silver_path or (SILVER_DIR / SILVER_CLEAN_FILE)

        if not self.silver_path.exists():
            raise FileNotFoundError(
                f"Silver data not found at {self.silver_path}. "
                "Run silver layer transformation first."
            )

    def load_clean_data(self) -> pd.DataFrame:
        """
        Load clean data from silver layer

        Returns:
            Cleaned DataFrame
        """
        df = pd.read_csv(self.silver_path, parse_dates=["date"])
        print(f"Loaded clean data from: {self.silver_path}")
        print(f"  Rows: {len(df)}, Date range: {df['date'].min()} to {df['date'].max()}")
        return df

    def calculate_annual_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate annual averages from monthly data

        Args:
            df: Silver layer DataFrame with monthly data

        Returns:
            DataFrame with annual averages in long format
        """
        print("\nCalculating annual averages...")

        # Group by year and component, calculate mean
        annual_avg = df.groupby(["year", "component"])["value_mbblpd"].mean().reset_index()

        # Rename for clarity
        annual_avg.columns = ["year", "component", "annual_avg_mbblpd"]

        print(f"  Calculated averages for {annual_avg['year'].nunique()} years")
        print(f"  Years: {annual_avg['year'].min()} - {annual_avg['year'].max()}")

        return annual_avg

    def pivot_to_wide_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pivot data to wide format (one column per component)

        Args:
            df: Annual averages in long format

        Returns:
            DataFrame in wide format with Year and individual component columns
        """
        print("Pivoting to wide format...")

        # Pivot: rows = years, columns = components, values = annual averages
        wide_df = df.pivot(
            index="year",
            columns="component",
            values="annual_avg_mbblpd"
        ).reset_index()

        # Ensure column order: Year, then Supply components, then Disposition components
        component_order = []

        # Add supply components (in order from SERIES_MAPPING)
        for comp in SERIES_MAPPING.values():
            if comp in SUPPLY_COMPONENTS and comp in wide_df.columns:
                component_order.append(comp)

        # Add disposition components
        for comp in SERIES_MAPPING.values():
            if comp in DISPOSITION_COMPONENTS and comp in wide_df.columns:
                component_order.append(comp)

        # Reorder columns
        wide_df = wide_df[["year"] + component_order]

        print(f"  Wide format: {len(wide_df)} rows x {len(wide_df.columns)} columns")

        return wide_df

    def add_balance_check(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add balance check columns (Supply vs Disposition)

        Supply components: Production + Imports + Net_Receipts
        Disposition components: Exports + Product_Supplied + Stock_Change

        In theory: Supply = Disposition (with small tolerance for rounding)

        Args:
            df: Wide format DataFrame

        Returns:
            DataFrame with added balance columns
        """
        print("Adding balance check columns...")

        # Calculate total supply (sum of supply components)
        supply_cols = [col for col in df.columns if col in SUPPLY_COMPONENTS]
        if supply_cols:
            df["Total_Supply"] = df[supply_cols].sum(axis=1)

        # Calculate total disposition (sum of disposition components)
        disposition_cols = [col for col in df.columns if col in DISPOSITION_COMPONENTS]
        if disposition_cols:
            df["Total_Disposition"] = df[disposition_cols].sum(axis=1)

        # Calculate balance (difference between supply and disposition)
        if "Total_Supply" in df.columns and "Total_Disposition" in df.columns:
            df["Balance_Difference"] = df["Total_Supply"] - df["Total_Disposition"]
            df["Balance_Pct_Diff"] = (
                df["Balance_Difference"] / df["Total_Supply"] * 100
            ).round(3)

        return df

    def save_annual_data(self, df: pd.DataFrame, output_path: Path = None) -> Path:
        """
        Save annual aggregated data to CSV

        Args:
            df: Annual DataFrame in wide format
            output_path: Path to save CSV file. If None, uses default

        Returns:
            Path to saved file
        """
        if output_path is None:
            output_path = GOLD_DIR / GOLD_ANNUAL_FILE

        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Round all numeric columns to 3 decimals
        df_rounded = df.copy()
        numeric_cols = df_rounded.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns
        for col in numeric_cols:
            if col != 'year':  # Don't round the year column
                df_rounded[col] = df_rounded[col].round(3)

        # Save to CSV
        df_rounded.to_csv(output_path, index=False)

        print(f"Annual data saved to: {output_path}")
        return output_path

    def run(self) -> pd.DataFrame:
        """
        Execute the complete gold layer aggregation

        Returns:
            Annual aggregated DataFrame in wide format
        """
        print("=" * 60)
        print("GOLD LAYER: Annual Aggregation & Analytics")
        print("=" * 60)

        # Load silver data
        clean_df = self.load_clean_data()

        # Calculate annual averages
        annual_avg = self.calculate_annual_averages(clean_df)

        # Pivot to wide format
        wide_df = self.pivot_to_wide_format(annual_avg)

        # Add balance check
        wide_df = self.add_balance_check(wide_df)

        # Save to gold layer
        self.save_annual_data(wide_df)

        print("\nGold layer aggregation complete!")
        print("=" * 60)

        return wide_df


def main():
    """Run gold layer aggregation as standalone script"""
    aggregation = GoldAggregation()
    df = aggregation.run()

    # Display summary
    print("\nAnnual Averages Summary:")
    print(df.head(10))


if __name__ == "__main__":
    main()
