"""
Sample usage examples for the NSE Options Data Pipeline.

This script demonstrates how to:
1. Load the processed data (CSV or JSON)
2. Filter by date, underlying, strike range, etc.
3. Perform basic queries for analysis.
"""

import pandas as pd
import os

def load_sample_data():
    """
    Load the sample data included in the repository.
    In practice, you would load the full processed dataset from
    processed_data/ or query the API/database.
    """
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'sample_data.csv')
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Sample data not found at {data_path}")
    df = pd.read_csv(data_path)
    # Convert timestamp if present
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def example_loading_and_inspection():
    """Example 1: Load data and inspect basic properties."""
    print("=== Example 1: Loading and Inspection ===")
    df = load_sample_data()
    print(f"Dataset shape: {df.shape}")
    print("\nColumn names:")
    print(df.columns.tolist())
    print("\nFirst 5 rows:")
    print(df.head())
    print("\nData types:")
    print(df.dtypes)
    print("\n")

def example_filtering():
    """Example 2: Filter data by various criteria."""
    print("=== Example 2: Filtering Data ===")
    df = load_sample_data()
    
    # Filter by underlying price range (if column exists)
    if 'underlying' in df.columns:
        df_underlying = df[(df['underlying'] >= 17500) & (df['underlying'] <= 18500)]
        print(f"Rows with underlying between 17500-18500: {len(df_underlying)}")
    
    # Filter by option type
    if 'option_type' in df.columns:
        calls = df[df['option_type'] == 'CE']
        puts = df[df['option_type'] == 'PE']
        print(f"Number of call options: {len(calls)}")
        print(f"Number of put options: {len(puts)}")
    
    # Filter by strike price range
    if 'strike' in df.columns:
        df_strike = df[(df['strike'] >= 17000) & (df['strike'] <= 19000)]
        print(f"Rows with strike between 17000-19000: {len(df_strike)}")
    
    # Filter by IV range (if available)
    if 'iv' in df.columns:
        df_iv = df[(df['iv'] >= 0.1) & (df['iv'] <= 0.6)]
        print(f"Rows with IV between 0.1 and 0.6: {len(df_iv)}")
    print("\n")

def example_basic_analysis():
    """Example 3: Perform simple analysis on the data."""
    print("=== Example 3: Basic Analysis ===")
    df = load_sample_data()
    
    if 'bid' in df.columns and 'ask' in df.columns:
        # Calculate mid price
        df['mid_price'] = (df['bid'] + df['ask']) / 2
        print(f"Average mid price: {df['mid_price'].mean():.2f}")
    
    if 'volume' in df.columns:
        print(f"Total volume: {df['volume'].sum():,}")
        print(f"Average volume per contract: {df['volume'].mean():.2f}")
    
    if 'open_interest' in df.columns:
        print(f"Total open interest: {df['open_interest'].sum():,}")
        print(f"Average open interest: {df['open_interest'].mean():.2f}")
    
    if 'iv' in df.columns:
        print(f"Average IV: {df['iv'].mean():.4f}")
        print(f"IV median: {df['iv'].median():.4f}")
    print("\n")

def example_api_usage():
    """Example 4: How to use the API (if running)."""
    print("=== Example 4: API Usage (Conceptual) ===")
    print("If the API is running (e.g., via uvicorn api:app), you can:")
    print("  - GET http://localhost:8000/  -> API info")
    print("  - GET http://localhost:8000/chain?date=2023-05-15 -> option chain for a date")
    print("  - GET http://localhost:8000/health -> health check")
    print("\n")

if __name__ == "__main__":
    print("NSE Options Data Pipeline - Sample Usage Examples\n")
    example_loading_and_inspection()
    example_filtering()
    example_basic_analysis()
    example_api_usage()
    print("All examples completed successfully.")