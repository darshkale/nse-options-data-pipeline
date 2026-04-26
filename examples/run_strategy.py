"""
Iron Condor Strategy Demo

This script demonstrates a simple iron condor strategy using the sample data.
It loads the sample data, constructs an iron condor for a given expiry,
and calculates PnL, win rate, and max drawdown.

Note: This uses the sample data (synthetic) for demonstration only.
For realistic backtesting, use the full dataset available on request.
"""

import pandas as pd
import numpy as np
import os

def load_sample_data():
    """
    Load the sample data included in the repository.
    """
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'sample_data.csv')
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Sample data not found at {data_path}")
    df = pd.read_csv(data_path)
    # Convert timestamp if present
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def run_iron_condor_demo(df):
    """
    Run a simplified iron condor strategy demonstration.
    Assumes the data contains a single expiry (or we take the first expiry).
    Steps:
    1. Determine underlying price (average of underlying column if present, else use a placeholder).
    2. Select OTM strikes for put and call spreads.
    3. Calculate the credit received from selling the inner strikes and buying the outer strikes.
    4. Simulate a range of underlying prices at expiry to compute PnL.
    5. Compute aggregate metrics: total PnL (average), win rate, max drawdown.
    """
    # For simplicity, we assume the data is for a single day and we use the first row's underlying.
    if 'underlying' in df.columns:
        underlying_price = df['underlying'].iloc[0]
    else:
        underlying_price = 18000.0  # fallback
    
    print(f"Underlying price used: {underlying_price:.2f}")
    
    # Define strike selection: OTM by a fixed amount (e.g., 100 points)
    otm_points = 100
    spread_width = 100  # width of each spread (buy strike - sell strike)
    
    put_sell_strike = underlying_price - otm_points
    put_buy_strike = put_sell_strike - spread_width
    call_sell_strike = underlying_price + otm_points
    call_buy_strike = call_sell_strike + spread_width
    
    print(f"Put Spread: Sell {put_sell_strike:.0f}, Buy {put_buy_strike:.0f}")
    print(f"Call Spread: Sell {call_sell_strike:.0f}, Buy {call_buy_strike:.0f}")
    
    # We need the mid prices for these strikes. We'll approximate by averaging bid and ask.
    # Since we have multiple rows, we'll find the closest strike for each type.
    def get_mid_price(strike, option_type):
        # Filter by option_type and find the row with strike closest to the desired strike
        if 'option_type' not in df.columns or 'strike' not in df.columns:
            # Fallback: use average mid price from the data
            if 'bid' in df.columns and 'ask' in df.columns:
                return (df['bid'].mean() + df['ask'].mean()) / 2
            else:
                return 50.0  # arbitrary
        mask = df['option_type'] == option_type
        if not mask.any():
            return 50.0
        df_opt = df[mask]
        # Find the index of the row with strike closest to the desired strike
        idx = (df_opt['strike'] - strike).abs().idxmin()
        actual_strike = df_opt.loc[idx, 'strike']
        bid = df_opt.loc[idx, 'bid'] if 'bid' in df_opt.columns else 0
        ask = df_opt.loc[idx, 'ask'] if 'ask' in df_opt.columns else 0
        if bid == 0 and ask == 0:
            # If no bid/ask, use a placeholder
            return 50.0
        return (bid + ask) / 2
    
    put_sell_mid = get_mid_price(put_sell_strike, 'PE')
    put_buy_mid = get_mid_price(put_buy_strike, 'PE')
    call_sell_mid = get_mid_price(call_sell_strike, 'CE')
    call_buy_mid = get_mid_price(call_buy_strike, 'CE')
    
    print(f"Put Sell Mid: {put_sell_mid:.2f}, Put Buy Mid: {put_buy_mid:.2f}")
    print(f"Call Sell Mid: {call_sell_mid:.2f}, Call Buy Mid: {call_buy_mid:.2f}")
    
    # Credit received: we sell the inner strikes (put_sell, call_sell) and buy the outer strikes (put_buy, call_buy)
    credit = (put_sell_mid + call_sell_mid) - (put_buy_mid + call_buy_mid)
    print(f"Net credit received: {credit:.2f}")
    
    # Now simulate a range of underlying prices at expiry to compute PnL.
    # We'll generate a range around the underlying price.
    sim_low = underlying_price * 0.8
    sim_high = underlying_price * 1.2
    sim_prices = np.linspace(sim_low, sim_high, 1000)
    
    def calculate_pnl(price):
        # Put spread PnL: we sold put_sell and bought put_buy
        put_pnl = -max(0, put_sell_strike - price) + max(0, put_buy_strike - price)
        # Call spread PnL: we sold call_sell and bought call_buy
        call_pnl = -max(0, price - call_sell_strike) + max(0, price - call_buy_strike)
        total_pnl = credit + put_pnl + call_pnl
        return total_pnl
    
    pnl_series = np.array([calculate_pnl(p) for p in sim_prices])
    
    total_pnl_avg = np.mean(pnl_series)
    win_rate = np.mean(pnl_series > 0) * 100
    max_drawdown = np.min(pnl_series)  # most negative
    
    print("\n--- Results ---")
    print(f"Average PnL per lot: {total_pnl_avg:.2f}")
    print(f"Win rate: {win_rate:.2f}%")
    print(f"Max drawdown: {max_drawdown:.2f}")
    print("\nNote: This is a simplified demonstration using synthetic data.")
    print("For realistic backtesting, use the full dataset with execution costs.")
    
    return {
        "average_pnl": total_pnl_avg,
        "win_rate": win_rate,
        "max_drawdown": max_drawdown,
        "credit": credit,
        "pnl_series": pnl_series
    }

if __name__ == "__main__":
    print("Iron Condor Strategy Demo")
    print("="*40)
    try:
        df = load_sample_data()
        print(f"Loaded {len(df)} rows of sample data.")
        print(f"Columns: {df.columns.tolist()}")
        print()
        run_iron_condor_demo(df)
    except Exception as e:
        print(f"Error: {e}")
        print("Please ensure the sample data exists in data/sample_data.csv")