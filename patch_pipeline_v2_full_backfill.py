#!/usr/bin/env python3
"""
PHASE 2B: FULL STRIKE GRID RECONSTRUCTION
=========================================================================
The real issue: Source data only has ±3 strikes, sparse distribution
Solution: Reconstruct FULL grid using Black-Scholes + Greeks interpolation
=========================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime
from scipy.optimize import fminbound
from scipy.stats import norm
import warnings
import os
warnings.filterwarnings('ignore')

class BlackScholesCalculator:
    """Calculate missing Greeks synthetically"""
    
    @staticmethod
    def d1(S, K, T, r, sigma):
        if T <= 0 or sigma <= 0:
            return np.nan
        return (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    
    @staticmethod
    def d2(d1, T, sigma):
        if T <= 0 or sigma <= 0:
            return np.nan
        return d1 - sigma*np.sqrt(T)
    
    @staticmethod
    def call_price(S, K, T, r, sigma):
        if T <= 0 or sigma <= 0:
            return max(S - K, 0)
        d1 = BlackScholesCalculator.d1(S, K, T, r, sigma)
        d2 = BlackScholesCalculator.d2(d1, T, sigma)
        return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)
    
    @staticmethod
    def call_delta(d1):
        if np.isnan(d1):
            return np.nan
        return norm.cdf(d1)
    
    @staticmethod
    def call_gamma(d1, S, T, sigma):
        if T <= 0 or sigma <= 0 or np.isnan(d1):
            return np.nan
        return norm.pdf(d1) / (S * sigma * np.sqrt(T))

def full_backfill_pipeline():
    """
    NEW STRATEGY:
    1. For each date/expiry, get existing ±3 data
    2. Fit IV smile curve through ±3 points
    3. Synthetically recreate ALL missing strikes from -10 to +10
    4. Use fitted IV smile + Black-Scholes to get Greeks
    5. Reconstruct LTP from fitted Greeks
    6. Bootstrap OI/volume
    """
    
    print("\n" + "="*90)
    print("🔨 PHASE 2B: FULL STRIKE GRID RECONSTRUCTION")
    print("="*90)
    
    # Load source
    print("\n📥 Loading source data...")
    df = pd.read_csv('nse-options-last-5-years/processed_data/nifty_atm_chain.csv')
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y')
    df['expiry'] = pd.to_datetime(df['expiry'], format='%d-%b-%Y')
    
    print(f"   Total records: {len(df):,}")
    print(f"   Current coverage: ±3 only")
    
    # Build full synthetic grid
    print("\n🔨 Reconstructing strike grids...")
    synthetic_rows = []
    
    # For each date/expiry combination
    groups = df.groupby(['date', 'expiry'])
    processed = 0
    
    for (trade_date, exp_date), group_df in groups:
        processed += 1
        if processed % 50 == 0:
            print(f"   Progress: {processed}/{len(groups)}")
        
        # Get reference parameters
        dte = group_df['dte'].iloc[0]
        underlying = group_df['underlying_price'].iloc[0]
        rate = group_df['interest_rate'].iloc[0] / 100.0
        atm = group_df['atm_strike'].iloc[0]
        
        # For each option type
        for opttype in ['CE', 'PE']:
            opttype_df = group_df[group_df['option_type'] == opttype]
            if len(opttype_df) == 0:
                continue
            
            # Get existing data points
            existing = {}
            for _, row in opttype_df.iterrows():
                offset = int(row['strike_offset'])
                existing[offset] = {
                    'strike': row['strike'],
                    'ltp': row['ltp'],
                    'iv': row['iv'],
                    'delta': row['delta'],
                    'gamma': row['gamma'],
                    'theta': row['theta'],
                    'vega': row['vega'],
                    'oi': row['open_interest'],
                    'vol': row['volume'],
                }
            
            # Fit IV smile through existing points
            if len(existing) >= 2:
                offsets_exist = sorted(existing.keys())
                ivs_exist = np.array([existing[off]['iv'] for off in offsets_exist])
                
                # Simple parabolic fit: IV(offset) = a + b*|offset| + c*offset^2
                # Use this to extrapolate ±4, ±5
                try:
                    coeffs = np.polyfit(offsets_exist, ivs_exist, 2)
                    iv_poly = np.poly1d(coeffs)
                except:
                    iv_poly = None
            else:
                iv_poly = None
            
            # Generate full grid from -10 to +10 (but focus on ±5 needed)
            for offset in range(-10, 11):
                if offset in existing:
                    # Use existing data
                    synth = existing[offset].copy()
                    synth['offset'] = offset
                    synth['_real'] = True
                else:
                    # Synthesize
                    strike = atm + offset * 50
                    
                    # IV estimation
                    if iv_poly is not None:
                        est_iv = float(iv_poly(offset))
                        est_iv = np.clip(est_iv, 0.1, 200)  # Realistic bounds
                    else:
                        # Default: use ATM IV + smile adjustment
                        atm_iv = existing[0]['iv'] if 0 in existing else 25
                        est_iv = atm_iv * (1 + 0.15 * abs(offset))
                        est_iv = np.clip(est_iv, 0.1, 200)
                    
                    # Greeks via Black-Scholes
                    T = max(dte / 365.0, 0.001)
                    
                    if opttype == 'CE':
                        est_ltp = BlackScholesCalculator.call_price(underlying, strike, T, rate, est_iv/100)
                        d1 = BlackScholesCalculator.d1(underlying, strike, T, rate, est_iv/100)
                        est_delta = BlackScholesCalculator.call_delta(d1)
                        est_gamma = BlackScholesCalculator.call_gamma(d1, underlying, T, est_iv/100)
                        est_theta = -underlying * norm.pdf(d1) * (est_iv/100) / (2*np.sqrt(T))
                        est_vega = underlying * norm.pdf(d1) * np.sqrt(T) / 100
                    else:  # PE
                        est_ltp = BlackScholesCalculator.call_price(underlying, strike, T, rate, est_iv/100) - underlying + strike*np.exp(-rate*T)
                        d1 = BlackScholesCalculator.d1(underlying, strike, T, rate, est_iv/100)
                        est_delta = BlackScholesCalculator.call_delta(d1) - 1  # Put delta
                        est_gamma = BlackScholesCalculator.call_gamma(d1, underlying, T, est_iv/100)
                        est_theta = underlying * norm.pdf(d1) * (est_iv/100) / (2*np.sqrt(T))
                        est_vega = underlying * norm.pdf(d1) * np.sqrt(T) / 100
                    
                    est_ltp = max(est_ltp, 0)
                    
                    # Bootstrap OI/volume from nearby real data
                    nearby_oi = []
                    nearby_vol = []
                    for nearby_off in [offset-1, offset-2, offset+1, offset+2]:
                        if nearby_off in existing:
                            nearby_oi.append(existing[nearby_off]['oi'])
                            nearby_vol.append(existing[nearby_off]['vol'])
                    
                    est_oi = np.mean(nearby_oi) if nearby_oi else 5000
                    est_vol = np.mean(nearby_vol) if nearby_vol else 50
                    
                    synth = {
                        'strike': strike,
                        'ltp': est_ltp,
                        'iv': est_iv,
                        'delta': est_delta,
                        'gamma': est_gamma,
                        'theta': est_theta,
                        'vega': est_vega,
                        'oi': est_oi,
                        'vol': est_vol,
                        'offset': offset,
                        '_real': False,
                    }
                
                # Add to output
                synth_row = opttype_df.iloc[0].copy()
                synth_row['strike'] = synth['strike']
                synth_row['strike_offset'] = synth['offset']
                synth_row['ltp'] = synth['ltp']
                synth_row['iv'] = synth['iv']
                synth_row['delta'] = synth['delta']
                synth_row['gamma'] = synth['gamma']
                synth_row['theta'] = synth['theta']
                synth_row['vega'] = synth['vega']
                synth_row['open_interest'] = synth['oi']
                synth_row['volume'] = synth['vol']
                synth_row['_synthetic'] = not synth['_real']
                
                synthetic_rows.append(synth_row)
    
    print(f"   ✅ Generated {len(synthetic_rows):,} full grid records")
    
    # Create new dataframe
    df_full = pd.DataFrame(synthetic_rows)
    
    # Verify coverage
    strike_coverage = df_full.groupby('date')['strike_offset'].agg(['min', 'max'])
    full_5 = ((strike_coverage['min'] <= -5) & (strike_coverage['max'] >= 5)).sum()
    full_10 = ((strike_coverage['min'] <= -10) & (strike_coverage['max'] >= 10)).sum()
    
    print(f"\n✅ COVERAGE IMPROVEMENT:")
    print(f"   Dates with ±10: {full_10} / {len(strike_coverage)} ({100*full_10/len(strike_coverage):.1f}%)")
    print(f"   Dates with ±5: {full_5} / {len(strike_coverage)} ({100*full_5/len(strike_coverage):.1f}%)")
    
    # Export
    output_file = './output/nifty_full_grid_backfilled.csv'
    df_full.to_csv(output_file, index=False)
    print(f"\n📄 Exported: {output_file}")
    print(f"   Records: {len(df_full):,}")
    print(f"   Synthetic: {(~df_full['_synthetic']).sum():,} real + {df_full['_synthetic'].sum():,} synthetic")
    
    return df_full

if __name__ == '__main__':
    df_full = full_backfill_pipeline()
    print("\n" + "="*90)
    print("✅ FULL STRIKE GRID RECONSTRUCTION COMPLETE")
    print("="*90)

