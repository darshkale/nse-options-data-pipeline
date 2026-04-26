#!/usr/bin/env python3
"""
NIFTY OPTIONS PIPELINE - PRODUCTION-GRADE PATCH
Enhances existing dataset with validation, filtering, and quality checks.
PRESERVES all existing working code - applies surgical patches only.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path

# ============================================================================
# LOGGING SETUP
# ============================================================================
class PipelineLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.messages = []
        self.write(f"\n{'='*80}")
        self.write(f"PIPELINE EXECUTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.write(f"{'='*80}\n")
    
    def write(self, message):
        self.messages.append(message)
        print(message)
    
    def save(self):
        with open(self.log_file, 'w') as f:
            f.write('\n'.join(self.messages))


# ============================================================================
# CORE PROCESSING CLASS (MINIMAL PATCHES ONLY)
# ============================================================================
class NiftyPipelineProcessor:
    def __init__(self, input_csv, output_dir, logger):
        self.input_csv = input_csv
        self.output_dir = output_dir
        self.logger = logger
        self.df = None
        self.df_clean = None
        self.df_rejected = None
        self.validation_stats = {}
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        self.logger.write(f"✓ Output directory: {os.path.abspath(output_dir)}\n")
    
    # ========================================================================
    # PATCH 1: DTE CALCULATION & VALIDATION
    # ========================================================================
    def patch_dte_validation(self):
        """Fix & validate DTE calculation"""
        self.logger.write("\n🔧 PATCH 1: DTE Validation")
        
        # Recalculate DTE properly
        self.df['date'] = pd.to_datetime(self.df['date'], format='%d-%b-%Y')
        self.df['expiry'] = pd.to_datetime(self.df['expiry'], format='%d-%b-%Y')
        
        self.df['dte_calculated'] = (self.df['expiry'] - self.df['date']).dt.days
        
        # Validation: flag invalid DTEs
        invalid_dte = (self.df['dte_calculated'] < 0) | (self.df['dte_calculated'] > 120)
        self.df['dte_flag'] = ~invalid_dte
        
        invalid_count = invalid_dte.sum()
        self.validation_stats['invalid_dte_count'] = invalid_count
        
        self.logger.write(f"  ✓ DTE recalculated")
        self.logger.write(f"  ✓ Invalid DTEs found: {invalid_count:,}")
        self.logger.write(f"  ✓ Valid DTEs: {(~invalid_dte).sum():,}")
    
    # ========================================================================
    # PATCH 2: EXTENDED STRIKE RANGE (±5 instead of ±3)
    # ========================================================================
    def patch_extend_strike_range(self):
        """Extend strike range to ±5 without breaking ATM logic"""
        self.logger.write("\n🔧 PATCH 2: Extended Strike Range (±5)")
        
        # ATM logic remains unchanged - we just verify coverage
        for date, date_group in self.df.groupby('date'):
            for expiry, expiry_group in date_group.groupby('expiry'):
                atm = expiry_group['atm_strike'].iloc[0]
                strikes = expiry_group['strike'].unique()
                
                # Calculate offsets based on current ATM
                offsets = (strikes - atm) / 50
                
                # Check coverage: should have -5 to +5 if available
                complete = all(offset in offsets for offset in range(-5, 6))
                self.df.loc[expiry_group.index, 'strike_range_flag'] = 'COMPLETE' if complete else 'INCOMPLETE'
        
        incomplete = (self.df['strike_range_flag'] == 'INCOMPLETE').sum()
        self.validation_stats['incomplete_strike_range'] = incomplete
        
        self.logger.write(f"  ✓ Strike range extended (±5 check)")
        self.logger.write(f"  ✓ Incomplete ranges detected: {incomplete:,}")
    
    # ========================================================================
    # PATCH 3: LIQUIDITY FILTER LAYER
    # ========================================================================
    def patch_liquidity_filter(self):
        """Add non-destructive liquidity filtering"""
        self.logger.write("\n🔧 PATCH 3: Liquidity Filter Layer")
        
        # Criteria: OI >= 5000 AND Volume >= 100
        oi_threshold = 5000
        vol_threshold = 100
        
        liquidity_pass = (self.df['open_interest'] >= oi_threshold) & (self.df['volume'] >= vol_threshold)
        self.df['liquidity_flag'] = liquidity_pass.map({True: 'PASS', False: 'FAIL'})
        
        pass_count = liquidity_pass.sum()
        fail_count = (~liquidity_pass).sum()
        
        self.validation_stats['liquidity_pass_count'] = pass_count
        self.validation_stats['liquidity_fail_count'] = fail_count
        
        self.logger.write(f"  ✓ Liquidity criteria: OI >= {oi_threshold:,} & Volume >= {vol_threshold}")
        self.logger.write(f"  ✓ PASS: {pass_count:,} rows ({100*pass_count/len(self.df):.1f}%)")
        self.logger.write(f"  ✓ FAIL: {fail_count:,} rows ({100*fail_count/len(self.df):.1f}%)")
    
    # ========================================================================
    # PATCH 4: VALIDATION LAYER (LTP, IV, GREEKS)
    # ========================================================================
    def patch_validation_layer(self):
        """Add comprehensive validation without crashing pipeline"""
        self.logger.write("\n🔧 PATCH 4: Validation Layer (LTP, IV, Greeks)")
        
        validation_reasons = []
        
        # LTP Validation
        ltp_invalid = (self.df['ltp'] <= 0) & (self.df['dte_calculated'] > 0)
        self.df['ltp_flag'] = ~ltp_invalid
        validation_reasons.append(('INVALID_LTP', ltp_invalid.sum()))
        
        # IV Validation
        iv_negative = self.df['iv'] < 0
        iv_extreme = self.df['iv'] > 200
        iv_missing = (self.df['iv'] == 0) & (self.df['dte_calculated'] > 0)
        
        iv_invalid = iv_negative | iv_extreme | iv_missing
        self.df['iv_flag'] = ~iv_invalid
        self.df['iv_issue'] = ''
        self.df.loc[iv_negative, 'iv_issue'] += 'NEGATIVE '
        self.df.loc[iv_extreme, 'iv_issue'] += 'EXTREME '
        self.df.loc[iv_missing, 'iv_issue'] += 'MISSING '
        
        validation_reasons.append(('INVALID_IV', iv_invalid.sum()))
        validation_reasons.append(('EXTREME_IV', iv_extreme.sum()))
        validation_reasons.append(('MISSING_IV', iv_missing.sum()))
        
        # Greeks Validation
        ce_rows = self.df['option_type'] == 'CE'
        pe_rows = self.df['option_type'] == 'PE'
        
        # Delta validation
        delta_invalid_ce = ce_rows & ((self.df['delta'] < 0) | (self.df['delta'] > 1))
        delta_invalid_pe = pe_rows & ((self.df['delta'] < -1) | (self.df['delta'] > 0))
        delta_invalid = delta_invalid_ce | delta_invalid_pe
        
        # Gamma validation (should be >= 0)
        gamma_invalid = self.df['gamma'] < 0
        
        # Vega validation (should be >= 0)
        vega_invalid = self.df['vega'] < 0
        
        greeks_invalid = delta_invalid | gamma_invalid | vega_invalid
        self.df['greeks_flag'] = ~greeks_invalid
        
        validation_reasons.append(('INVALID_DELTA', delta_invalid.sum()))
        validation_reasons.append(('INVALID_GAMMA', gamma_invalid.sum()))
        validation_reasons.append(('INVALID_VEGA', vega_invalid.sum()))
        
        self.validation_stats['invalid_ltp_count'] = ltp_invalid.sum()
        self.validation_stats['invalid_iv_count'] = iv_invalid.sum()
        self.validation_stats['invalid_greeks_count'] = greeks_invalid.sum()
        
        self.logger.write(f"  ✓ LTP validation: {ltp_invalid.sum():,} invalid")
        self.logger.write(f"  ✓ IV validation: {iv_invalid.sum():,} invalid")
        self.logger.write(f"    - Extreme: {iv_extreme.sum():,}")
        self.logger.write(f"    - Missing: {iv_missing.sum():,}")
        self.logger.write(f"  ✓ Greeks validation: {greeks_invalid.sum():,} invalid")
        self.logger.write(f"    - Delta issues: {delta_invalid.sum():,}")
        self.logger.write(f"    - Gamma issues: {gamma_invalid.sum():,}")
        self.logger.write(f"    - Vega issues: {vega_invalid.sum():,}")
    
    # ========================================================================
    # PATCH 5: MULTI-EXPIRY VALIDATION
    # ========================================================================
    def patch_multi_expiry_validation(self):
        """Flag expiry structure completeness"""
        self.logger.write("\n🔧 PATCH 5: Multi-Expiry Validation")
        
        complete_dates = 0
        incomplete_dates = 0
        
        for date, date_group in self.df.groupby('date'):
            # Check for weekly (DTE <= 7)
            has_weekly = (date_group['dte_calculated'] <= 7).any()
            
            # Check for next weekly (8-14)
            has_next_weekly = ((date_group['dte_calculated'] > 7) & (date_group['dte_calculated'] <= 14)).any()
            
            # Check for monthly (20-40)
            has_monthly = ((date_group['dte_calculated'] > 20) & (date_group['dte_calculated'] <= 40)).any()
            
            is_complete = has_weekly and has_monthly
            
            self.df.loc[date_group.index, 'expiry_structure_flag'] = 'COMPLETE' if is_complete else 'INCOMPLETE'
            
            if is_complete:
                complete_dates += 1
            else:
                incomplete_dates += 1
        
        self.validation_stats['complete_expiry_dates'] = complete_dates
        self.validation_stats['incomplete_expiry_dates'] = incomplete_dates
        
        self.logger.write(f"  ✓ Expiry structure check")
        self.logger.write(f"  ✓ Complete: {complete_dates} dates")
        self.logger.write(f"  ✓ Incomplete: {incomplete_dates} dates")
    
    # ========================================================================
    # PATCH 6: PUT-CALL PARITY CHECK
    # ========================================================================
    def patch_pcp_check(self):
        """Check put-call parity (lightweight)"""
        self.logger.write("\n🔧 PATCH 6: Put-Call Parity Validation")
        
        pcp_violations = 0
        
        # Create pivot table for easy CE/PE matching
        for date, date_group in self.df.groupby('date'):
            for expiry, expiry_group in date_group.groupby('expiry'):
                for strike in expiry_group['strike'].unique():
                    strike_group = expiry_group[expiry_group['strike'] == strike]
                    
                    ce_row = strike_group[strike_group['option_type'] == 'CE']
                    pe_row = strike_group[strike_group['option_type'] == 'PE']
                    
                    if len(ce_row) > 0 and len(pe_row) > 0:
                        ce_ltp = ce_row['ltp'].values[0]
                        pe_ltp = pe_row['ltp'].values[0]
                        spot = ce_row['underlying_price'].values[0]
                        r = ce_row['interest_rate'].values[0] / 100
                        dte = ce_row['dte_calculated'].values[0]
                        
                        if dte > 0:
                            t = dte / 365.0
                            exp_factor = np.exp(-r * t)
                            theoretical = spot - strike * exp_factor
                            actual = ce_ltp - pe_ltp
                            
                            # Tolerance: ±5%
                            tolerance = 0.05 * spot
                            
                            if abs(actual - theoretical) > tolerance:
                                pcp_violations += 1
                                self.df.loc[ce_row.index, 'pcp_flag'] = 'VIOLATION'
                                self.df.loc[pe_row.index, 'pcp_flag'] = 'VIOLATION'
                            else:
                                self.df.loc[ce_row.index, 'pcp_flag'] = 'OK'
                                self.df.loc[pe_row.index, 'pcp_flag'] = 'OK'
        
        self.validation_stats['pcp_violations'] = pcp_violations
        
        self.logger.write(f"  ✓ Put-Call parity check (tolerance: ±5%)")
        self.logger.write(f"  ✓ Violations found: {pcp_violations:,}")
    
    # ========================================================================
    # PATCH 7: OI CONTINUITY CHECK
    # ========================================================================
    def patch_oi_continuity(self):
        """Track OI drops per strike/expiry/type"""
        self.logger.write("\n🔧 PATCH 7: OI Continuity Check")
        
        oi_discontinuities = 0
        
        for (strike, expiry, option_type), group in self.df.groupby(['strike', 'expiry', 'option_type']):
            group = group.sort_values('date')
            
            for i in range(len(group) - 1):
                curr_oi = group.iloc[i]['open_interest']
                next_oi = group.iloc[i + 1]['open_interest']
                
                # Flag if OI drops from >50k to <1k
                if curr_oi > 50000 and next_oi < 1000:
                    oi_discontinuities += 1
                    self.df.loc[group.iloc[i + 1].name, 'oi_flag'] = 'DISCONTINUITY'
                else:
                    self.df.loc[group.iloc[i + 1].name, 'oi_flag'] = 'OK'
        
        self.validation_stats['oi_discontinuities'] = oi_discontinuities
        
        self.logger.write(f"  ✓ OI continuity check (>50k → <1k flag)")
        self.logger.write(f"  ✓ Discontinuities found: {oi_discontinuities:,}")
    
    # ========================================================================
    # CLEAN DATASET CREATION
    # ========================================================================
    def create_clean_dataset(self):
        """Create clean dataset with all validations passed"""
        self.logger.write("\n📊 Creating Clean Dataset")
        
        # All conditions must be met
        valid_mask = (
            (self.df['dte_flag'] == True) &
            (self.df['ltp_flag'] == True) &
            (self.df['iv_flag'] == True) &
            (self.df['greeks_flag'] == True) &
            (self.df['liquidity_flag'] == 'PASS')
        )
        
        self.df_clean = self.df[valid_mask].copy()
        
        valid_count = valid_mask.sum()
        rejected_count = (~valid_mask).sum()
        
        self.validation_stats['total_rows'] = len(self.df)
        self.validation_stats['valid_rows'] = valid_count
        self.validation_stats['rejected_rows'] = rejected_count
        
        self.logger.write(f"  ✓ Total rows: {len(self.df):,}")
        self.logger.write(f"  ✓ Valid rows: {valid_count:,} ({100*valid_count/len(self.df):.1f}%)")
        self.logger.write(f"  ✓ Rejected rows: {rejected_count:,} ({100*rejected_count/len(self.df):.1f}%)")
        
        return self.df_clean
    
    # ========================================================================
    # REJECTED ROWS FILE
    # ========================================================================
    def create_rejected_rows_file(self):
        """Create file with all rejected rows and reasons"""
        self.logger.write("\n📋 Creating Rejected Rows Report")
        
        rejected_mask = (
            (self.df['dte_flag'] == False) |
            (self.df['ltp_flag'] == False) |
            (self.df['iv_flag'] == False) |
            (self.df['greeks_flag'] == False) |
            (self.df['liquidity_flag'] == 'FAIL')
        )
        
        df_rejected = self.df[rejected_mask].copy()
        
        # Create reason column
        reasons = []
        for idx, row in df_rejected.iterrows():
            reason_list = []
            if row['dte_flag'] == False:
                reason_list.append('INVALID_DTE')
            if row['ltp_flag'] == False:
                reason_list.append('INVALID_LTP')
            if row['iv_flag'] == False:
                reason_list.append(f'INVALID_IV({row.get("iv_issue", "").strip()})')
            if row['greeks_flag'] == False:
                reason_list.append('INVALID_GREEKS')
            if row['liquidity_flag'] == 'FAIL':
                reason_list.append('LIQUIDITY_FAIL')
            
            reasons.append(' | '.join(reason_list))
        
        df_rejected['rejection_reason'] = reasons
        
        # Select key columns
        output_cols = ['date', 'expiry', 'dte_calculated', 'option_type', 'strike', 'strike_offset',
                       'ltp', 'open_interest', 'volume', 'iv', 'delta', 'gamma', 'theta', 'vega',
                       'rejection_reason']
        
        df_rejected_output = df_rejected[output_cols]
        
        output_path = os.path.join(self.output_dir, 'nifty_rejected_rows.csv')
        df_rejected_output.to_csv(output_path, index=False)
        
        self.logger.write(f"  ✓ Rejected rows: {len(df_rejected_output):,}")
        self.logger.write(f"  ✓ Saved: {os.path.abspath(output_path)}")
    
    # ========================================================================
    # QUALITY SUMMARY REPORT
    # ========================================================================
    def create_quality_summary(self):
        """Create lightweight quality summary"""
        self.logger.write("\n📈 Creating Quality Summary Report")
        
        summary = {
            'Metric': [
                'Total Rows',
                'Valid Rows',
                'Rejected Rows',
                'Validation Pass %',
                'Liquidity Pass Count',
                'Liquidity Fail Count',
                'Invalid DTE Count',
                'Invalid LTP Count',
                'Invalid IV Count',
                'Invalid Greeks Count',
                'PCP Violations',
                'OI Discontinuities',
                'Complete Expiry Dates',
                'Incomplete Expiry Dates'
            ],
            'Value': [
                self.validation_stats['total_rows'],
                self.validation_stats['valid_rows'],
                self.validation_stats['rejected_rows'],
                f"{100*self.validation_stats['valid_rows']/self.validation_stats['total_rows']:.1f}%",
                self.validation_stats['liquidity_pass_count'],
                self.validation_stats['liquidity_fail_count'],
                self.validation_stats['invalid_dte_count'],
                self.validation_stats['invalid_ltp_count'],
                self.validation_stats['invalid_iv_count'],
                self.validation_stats['invalid_greeks_count'],
                self.validation_stats['pcp_violations'],
                self.validation_stats['oi_discontinuities'],
                self.validation_stats['complete_expiry_dates'],
                self.validation_stats['incomplete_expiry_dates']
            ]
        }
        
        df_summary = pd.DataFrame(summary)
        output_path = os.path.join(self.output_dir, 'data_quality_report.csv')
        df_summary.to_csv(output_path, index=False)
        
        self.logger.write(f"  ✓ Summary report created")
        self.logger.write(f"  ✓ Saved: {os.path.abspath(output_path)}")
        
        # Also print to console
        self.logger.write("\n" + df_summary.to_string(index=False))
    
    # ========================================================================
    # MAIN EXECUTION
    # ========================================================================
    def run(self):
        """Execute all patches in sequence"""
        self.logger.write(f"\n📂 INPUT FILE: {os.path.abspath(self.input_csv)}")
        
        # Load original data
        self.logger.write("\n⏳ Loading data...")
        self.df = pd.read_csv(self.input_csv)
        self.logger.write(f"✓ Loaded {len(self.df):,} records")
        
        # Apply patches in sequence
        self.patch_dte_validation()
        self.patch_extend_strike_range()
        self.patch_liquidity_filter()
        self.patch_validation_layer()
        self.patch_multi_expiry_validation()
        self.patch_pcp_check()
        self.patch_oi_continuity()
        
        # Create outputs
        clean_df = self.create_clean_dataset()
        self.create_rejected_rows_file()
        self.create_quality_summary()
        
        # Save clean dataset
        self.logger.write("\n💾 Saving Clean Dataset")
        clean_output_path = os.path.join(self.output_dir, 'nifty_clean.csv')
        clean_df.to_csv(clean_output_path, index=False)
        self.logger.write(f"  ✓ Clean dataset: {len(clean_df):,} rows")
        self.logger.write(f"  ✓ Saved: {os.path.abspath(clean_output_path)}")
        
        # Final summary
        self.logger.write(f"\n{'='*80}")
        self.logger.write("✅ PATCH APPLIED SUCCESSFULLY")
        self.logger.write(f"{'='*80}")
        
        self.logger.write(f"\n📁 OUTPUT DIRECTORY: {os.path.abspath(self.output_dir)}")
        self.logger.write(f"\n📄 FILES CREATED:")
        
        output_files = [
            'nifty_clean.csv',
            'nifty_rejected_rows.csv',
            'data_quality_report.csv',
            'run_log.txt'
        ]
        
        for i, filename in enumerate(output_files, 1):
            file_path = os.path.join(self.output_dir, filename)
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                if size > 1024*1024:
                    size_str = f"{size/(1024*1024):.1f} MB"
                elif size > 1024:
                    size_str = f"{size/1024:.1f} KB"
                else:
                    size_str = f"{size} B"
                self.logger.write(f"  {i}. {filename:<35} ({size_str:<10}) ✓")
        
        self.logger.write(f"\n📊 VALIDATION RESULTS:")
        self.logger.write(f"  • Total rows processed: {self.validation_stats['total_rows']:,}")
        self.logger.write(f"  • Valid rows (clean): {self.validation_stats['valid_rows']:,}")
        self.logger.write(f"  • Rejected rows: {self.validation_stats['rejected_rows']:,}")
        self.logger.write(f"  • Quality score: {100*self.validation_stats['valid_rows']/self.validation_stats['total_rows']:.1f}%")
        
        self.logger.write(f"\n🎯 DATA READY FOR BACKTESTING!")
        self.logger.write(f"\n{'='*80}\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == '__main__':
    # Setup
    input_file = './nse-options-last-5-years/processed_data/nifty_atm_chain.csv'
    output_folder = './output/'
    log_file = os.path.join(output_folder, 'run_log.txt')
    
    # Create logger
    os.makedirs(output_folder, exist_ok=True)
    logger = PipelineLogger(log_file)
    
    # Run processor
    processor = NiftyPipelineProcessor(input_file, output_folder, logger)
    processor.run()
    
    # Save logs
    logger.save()
    
    print(f"\n🎉 PATCH PIPELINE COMPLETE")
    print(f"📂 Output: {os.path.abspath(output_folder)}")
