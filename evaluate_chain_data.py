#!/usr/bin/env python3
"""
NIFTY Options Chain - Data Quality Evaluator
Comprehensive analysis and quality flagging for 5-year options data
"""

import pandas as pd
import numpy as np
from scipy.stats import norm
from datetime import datetime, timedelta
import argparse
import os
import warnings
warnings.filterwarnings('ignore')


class ChainDataEvaluator:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.df = None
        self.flags_detail = []
        self.coverage_by_date = {}
        self.load_data()
        
    def load_data(self):
        """Load and parse the CSV"""
        self.df = pd.read_csv(self.csv_path)
        # Standardize column names
        self.df.columns = ['date', 'expiry', 'dte', 'optiontype', 'atmstrike', 
                          'strike', 'strikeoffset', 'underlyingprice', 'interestrate',
                          'ltp', 'openinterest', 'oichange', 'volume', 'iv', 
                          'delta', 'gamma', 'theta', 'vega']
        
        # Parse dates
        self.df['date'] = pd.to_datetime(self.df['date'], format='%d-%b-%Y')
        self.df['expiry'] = pd.to_datetime(self.df['expiry'], format='%d-%b-%Y')
        
        print(f"✅ Loaded {len(self.df):,} rows from {self.csv_path}")
        return self.df
    
    def flag_issue(self, row_idx, strike, expiry, optiontype, flag_type, flag_value=""):
        """Record a data quality flag"""
        date = self.df.loc[row_idx, 'date']
        self.flags_detail.append({
            'date': date.strftime('%d-%b-%Y'),
            'strike': strike,
            'expiry': expiry.strftime('%d-%b-%Y'),
            'optiontype': optiontype,
            'flag_type': flag_type,
            'flag_value': str(flag_value)
        })
    
    # ========== SECTION 1: DATA COVERAGE ANALYSIS ==========
    def section_1_coverage(self):
        print("\n" + "="*70)
        print("📊 SECTION 1: DATA COVERAGE ANALYSIS")
        print("="*70)
        
        # 1a. Date Range
        first_date = self.df['date'].min()
        last_date = self.df['date'].max()
        calendar_days = (last_date - first_date).days + 1
        unique_dates = self.df['date'].nunique()
        years_spanned = (last_date.year - first_date.year) + 1
        expected_trading_days = 250 * years_spanned
        coverage_pct = (unique_dates / expected_trading_days) * 100
        
        print(f"\n1a. DATE RANGE:")
        print(f"   First Date: {first_date.strftime('%d-%b-%Y')}")
        print(f"   Last Date:  {last_date.strftime('%d-%b-%Y')}")
        print(f"   Calendar Days: {calendar_days}")
        print(f"   Unique Trading Dates: {unique_dates}")
        print(f"   Expected Trading Days (~250/yr): {expected_trading_days}")
        print(f"   Coverage: {coverage_pct:.1f}%")
        
        # 1b. Per-Year Breakdown
        print(f"\n1b. PER-YEAR BREAKDOWN:")
        yearly = self.df.groupby(self.df['date'].dt.year).agg({
            'date': 'nunique',
            'expiry': 'nunique',
            'strike': 'count',
        })
        
        yearly['strikes_per_date'] = (yearly['strike'] / yearly['date']).round(1)
        yearly = yearly.rename(columns={
            'date': 'Trading Days',
            'expiry': 'Unique Expiries',
            'strike': 'Total Rows'
        })
        yearly['Year'] = yearly.index
        yearly = yearly[['Year', 'Trading Days', 'Unique Expiries', 'Total Rows', 'strikes_per_date']]
        
        print(yearly.to_string(index=False))
        
        # 1c. Expiry Coverage
        unique_expiries = self.df['expiry'].nunique()
        weekly = len(self.df[self.df['dte'] <= 7]['expiry'].unique())
        monthly = len(self.df[(self.df['dte'] > 7) & (self.df['dte'] <= 35)]['expiry'].unique())
        far = len(self.df[self.df['dte'] > 60]['expiry'].unique())
        max_dte = self.df['dte'].max()
        
        print(f"\n1c. EXPIRY COVERAGE:")
        print(f"   Total Unique Expiries: {unique_expiries}")
        print(f"   Weekly Expiries (DTE ≤ 7): {weekly}")
        print(f"   Monthly Expiries (7 < DTE ≤ 35): {monthly}")
        print(f"   Far Expiries (DTE > 60): {far}")
        print(f"   Max DTE Found: {max_dte}")
        
        return {
            'first_date': first_date,
            'last_date': last_date,
            'calendar_days': calendar_days,
            'unique_dates': unique_dates,
            'expected_trading_days': expected_trading_days,
            'coverage_pct': coverage_pct,
            'yearly': yearly
        }
    
    # ========== SECTION 2: STRIKE COVERAGE ==========
    def section_2_strikes(self):
        print("\n" + "="*70)
        print("⚡ SECTION 2: STRIKE COVERAGE")
        print("="*70)
        
        incomplete_dates = []
        imbalanced_dates = []
        
        print(f"\n2a. STRIKE RANGE ANALYSIS:")
        min_offset = self.df['strikeoffset'].min()
        max_offset = self.df['strikeoffset'].max()
        print(f"   Min Strike Offset: {min_offset}")
        print(f"   Max Strike Offset: {max_offset}")
        
        # Count dates with full ±3 coverage
        full_coverage = 0
        partial_coverage = 0
        
        for date, group in self.df.groupby('date'):
            offsets = group['strikeoffset'].unique()
            unique_strikes = len(group['strike'].unique())
            
            if unique_strikes < 5:
                incomplete_dates.append((date, unique_strikes))
            
            if set(range(-3, 4)).issubset(set(offsets)):
                full_coverage += 1
            else:
                partial_coverage += 1
        
        print(f"   Dates with Full ±3 Range: {full_coverage}")
        print(f"   Dates with Partial Coverage: {partial_coverage}")
        
        if incomplete_dates:
            print(f"   ⚠️  INCOMPLETE Coverage ({len(incomplete_dates)} dates):")
            for date, count in incomplete_dates[:5]:
                print(f"       {date.strftime('%d-%b-%Y')}: {count} strikes")
            if len(incomplete_dates) > 5:
                print(f"       ... and {len(incomplete_dates) - 5} more")
        
        print(f"\n2b. CE vs PE BALANCE:")
        for date, group in self.df.groupby('date'):
            ce_count = len(group[group['optiontype'] == 'CE'])
            pe_count = len(group[group['optiontype'] == 'PE'])
            
            if abs(ce_count - pe_count) > 2:
                imbalanced_dates.append((date, ce_count, pe_count))
        
        if imbalanced_dates:
            print(f"   ⚠️  IMBALANCED Dates ({len(imbalanced_dates)} dates):")
            for date, ce, pe in imbalanced_dates[:5]:
                print(f"       {date.strftime('%d-%b-%Y')}: CE={ce}, PE={pe}")
            if len(imbalanced_dates) > 5:
                print(f"       ... and {len(imbalanced_dates) - 5} more")
        else:
            print(f"   ✓ All dates well-balanced")
        
        return {
            'incomplete_dates': incomplete_dates,
            'imbalanced_dates': imbalanced_dates,
            'full_coverage': full_coverage,
            'partial_coverage': partial_coverage
        }
    
    # ========== SECTION 3: FIELD QUALITY CHECKS ==========
    def section_3_field_quality(self):
        print("\n" + "="*70)
        print("🔍 SECTION 3: FIELD QUALITY CHECKS")
        print("="*70)
        
        issues = {
            'zero_ltp': 0, 'suspicious_ltp': 0, 'null_ltp': 0,
            'negative_oi': 0, 'zero_oi_mid': 0, 'null_oi': 0,
            'negative_iv': 0, 'extreme_iv': 0, 'missing_iv': 0, 'null_iv': 0,
            'bad_delta_ce': 0, 'bad_delta_pe': 0,
            'negative_gamma': 0, 'positive_theta': 0, 'negative_vega': 0,
        }
        
        # 3a. LTP Quality
        print(f"\n3a. LTP QUALITY:")
        for idx, row in self.df.iterrows():
            if pd.isna(row['ltp']):
                issues['null_ltp'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'NULL_LTP')
            elif row['ltp'] <= 0 and row['dte'] > 0:
                issues['zero_ltp'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'ZERO_LTP', row['ltp'])
            elif row['ltp'] > row['underlyingprice'] * 1.5:
                issues['suspicious_ltp'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'SUSPICIOUS_LTP', row['ltp'])
        
        print(f"   Zero/Negative LTP (dte>0): {issues['zero_ltp']}")
        print(f"   Suspicious LTP (>1.5×spot): {issues['suspicious_ltp']}")
        print(f"   Null LTP: {issues['null_ltp']}")
        
        # 3b. OI Quality
        print(f"\n3b. OPEN INTEREST QUALITY:")
        for idx, row in self.df.iterrows():
            if pd.isna(row['openinterest']):
                issues['null_oi'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'NULL_OI')
            elif row['openinterest'] < 0:
                issues['negative_oi'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'NEGATIVE_OI', row['openinterest'])
            elif row['openinterest'] == 0 and row['dte'] > 1:
                issues['zero_oi_mid'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'ZERO_OI_MID_LIFE')
        
        print(f"   Negative OI: {issues['negative_oi']}")
        print(f"   Zero OI (mid-life): {issues['zero_oi_mid']}")
        print(f"   Null OI: {issues['null_oi']}")
        
        # 3c. IV Quality
        print(f"\n3c. IMPLIED VOLATILITY QUALITY:")
        for idx, row in self.df.iterrows():
            if pd.isna(row['iv']):
                issues['null_iv'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'NULL_IV')
            elif row['iv'] < 0:
                issues['negative_iv'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'NEGATIVE_IV', row['iv'])
            elif row['iv'] > 200:
                issues['extreme_iv'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'EXTREME_IV', f"{row['iv']:.1f}%")
            elif row['iv'] == 0 and row['dte'] > 0:
                issues['missing_iv'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'MISSING_IV')
        
        print(f"   Negative IV: {issues['negative_iv']}")
        print(f"   Extreme IV (>200%): {issues['extreme_iv']}")
        print(f"   Missing IV (dte>0): {issues['missing_iv']}")
        print(f"   Null IV: {issues['null_iv']}")
        
        # 3d. Greeks Quality
        print(f"\n3d. GREEKS QUALITY:")
        for idx, row in self.df.iterrows():
            if row['optiontype'] == 'CE':
                if not (0 <= row['delta'] <= 1) and row['dte'] > 0:
                    issues['bad_delta_ce'] += 1
                    self.flag_issue(idx, row['strike'], row['expiry'], 'CE', 'BAD_DELTA_CE', f"{row['delta']:.3f}")
            else:  # PE
                if not (-1 <= row['delta'] <= 0) and row['dte'] > 0:
                    issues['bad_delta_pe'] += 1
                    self.flag_issue(idx, row['strike'], row['expiry'], 'PE', 'BAD_DELTA_PE', f"{row['delta']:.3f}")
            
            if row['gamma'] < 0 and row['dte'] > 0:
                issues['negative_gamma'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'NEGATIVE_GAMMA', f"{row['gamma']:.6f}")
            
            if row['theta'] > 0 and row['dte'] > 0:
                issues['positive_theta'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'POSITIVE_THETA', f"{row['theta']:.3f}")
            
            if row['vega'] < 0 and row['dte'] > 0:
                issues['negative_vega'] += 1
                self.flag_issue(idx, row['strike'], row['expiry'], row['optiontype'], 'NEGATIVE_VEGA', f"{row['vega']:.6f}")
        
        print(f"   Bad Delta (CE): {issues['bad_delta_ce']}")
        print(f"   Bad Delta (PE): {issues['bad_delta_pe']}")
        print(f"   Negative Gamma: {issues['negative_gamma']}")
        print(f"   Positive Theta: {issues['positive_theta']}")
        print(f"   Negative Vega: {issues['negative_vega']}")
        
        return issues
    
    # ========== SECTION 4: TIME SERIES CONTINUITY ==========
    def section_4_continuity(self):
        print("\n" + "="*70)
        print("📈 SECTION 4: TIME SERIES CONTINUITY")
        print("="*70)
        
        # 4a. Missing Dates
        print(f"\n4a. MISSING DATE DETECTION:")
        unique_dates = sorted(self.df['date'].unique())
        gaps = []
        
        for i in range(len(unique_dates) - 1):
            gap_days = (unique_dates[i+1] - unique_dates[i]).days
            if gap_days > 3:
                gaps.append({
                    'gap_start': unique_dates[i],
                    'gap_end': unique_dates[i+1],
                    'gap_days': gap_days
                })
        
        if gaps:
            print(f"   Found {len(gaps)} gaps > 3 days:")
            for gap in gaps[:10]:
                flag = "🚨" if gap['gap_days'] > 7 else "⚠️"
                print(f"   {flag} {gap['gap_start'].strftime('%d-%b-%Y')} → {gap['gap_end'].strftime('%d-%b-%Y')}: {gap['gap_days']} days")
            if len(gaps) > 10:
                print(f"   ... and {len(gaps) - 10} more")
        else:
            print(f"   ✓ No significant gaps found")
        
        # 4b. OI Continuity
        print(f"\n4b. OPEN INTEREST CONTINUITY:")
        oi_drops = 0
        for (strike, expiry, optiontype), group in self.df.groupby(['strike', 'expiry', 'optiontype']):
            group = group.sort_values('date')
            for i in range(len(group) - 1):
                curr_oi = group.iloc[i]['openinterest']
                next_oi = group.iloc[i+1]['openinterest']
                if curr_oi > 50000 and next_oi == 0:
                    oi_drops += 1
        
        print(f"   Sudden OI Drops (>50k → 0): {oi_drops}")
        if oi_drops > 0:
            print(f"   ⚠️  May indicate data gaps or contract rollovers")
        
        # 4c. IV Surface
        print(f"\n4c. IV SURFACE SMOOTHNESS (Latest Date):")
        latest_date = self.df['date'].max()
        latest = self.df[self.df['date'] == latest_date].copy()
        
        if len(latest) > 0:
            # IV by DTE bucket
            latest['dte_bucket'] = pd.cut(latest['dte'], bins=[0, 7, 30, 60, 999], 
                                         labels=['0-7d', '8-30d', '31-60d', '61+d'])
            iv_by_dte = latest.groupby('dte_bucket')['iv'].mean()
            print(f"   Avg IV by DTE Bucket:")
            for bucket, iv in iv_by_dte.items():
                print(f"       {bucket}: {iv:.2f}%")
            
            # IV smile check (ATM vs OTM)
            atm_rows = latest[latest['strikeoffset'] == 0]
            if len(atm_rows) > 0:
                atm_iv = atm_rows['iv'].mean()
                otm_iv = latest[latest['strikeoffset'].abs() >= 1]['iv'].mean()
                
                smile_ratio = otm_iv / atm_iv if atm_iv > 0 else 0
                if otm_iv < atm_iv:
                    print(f"   ⚠️  Inverted IV Smile (ATM={atm_iv:.2f}%, OTM={otm_iv:.2f}%)")
                else:
                    print(f"   ✓ Normal IV Shape (ATM={atm_iv:.2f}%, OTM={otm_iv:.2f}%)")
        
        return {
            'gaps': gaps,
            'oi_drops': oi_drops
        }
    
    # ========== SECTION 5: SUMMARY SCORECARD ==========
    def section_5_scorecard(self, coverage_data, quality_issues, continuity_data):
        print("\n" + "="*70)
        print("📋 SECTION 5: SUMMARY SCORECARD")
        print("="*70)
        
        total_rows = len(self.df)
        total_flags = len(self.flags_detail)
        
        # Calculate percentages
        zero_ltp_pct = (quality_issues['zero_ltp'] / total_rows * 100) if total_rows > 0 else 0
        missing_iv_pct = (quality_issues['missing_iv'] / total_rows * 100) if total_rows > 0 else 0
        extreme_iv_pct = (quality_issues['extreme_iv'] / total_rows * 100) if total_rows > 0 else 0
        bad_greeks = (quality_issues['bad_delta_ce'] + quality_issues['bad_delta_pe'] + 
                      quality_issues['negative_gamma'] + quality_issues['positive_theta'] + 
                      quality_issues['negative_vega'])
        bad_greeks_pct = (bad_greeks / total_rows * 100) if total_rows > 0 else 0
        
        # Grade calculation
        coverage_pct = coverage_data['coverage_pct']
        
        if coverage_pct > 95 and missing_iv_pct < 5 and zero_ltp_pct < 2:
            grade = 'A'
            verdict = "Excellent - Ready for backtesting"
        elif coverage_pct > 85 and missing_iv_pct < 15 and zero_ltp_pct < 5:
            grade = 'B'
            verdict = "Good - Minor data gaps, usable"
        elif coverage_pct > 70:
            grade = 'C'
            verdict = "Fair - Moderate issues, proceed with caution"
        elif coverage_pct > 50:
            grade = 'D'
            verdict = "Poor - Significant data gaps"
        else:
            grade = 'F'
            verdict = "Unsuitable - Major data quality issues"
        
        print("\n╔══════════════════════════════════════════════════════════╗")
        print("║        NIFTY OPTIONS CHAIN — DATA QUALITY REPORT        ║")
        print("╠══════════════════════════════════════════════════════════╣")
        print(f"║ Date Range    : {coverage_data['first_date'].strftime('%d-%b-%Y')} → {coverage_data['last_date'].strftime('%d-%b-%Y'):<22}   ║")
        print(f"║ Trading Days  : {coverage_data['unique_dates']} / ~{coverage_data['expected_trading_days']} expected ({coverage_pct:.0f}%) ║")
        print(f"║ Total Rows    : {total_rows:,}".ljust(55) + "║")
        print(f"║ Unique Strikes: {self.df['strike'].nunique()}".ljust(55) + "║")
        print(f"║ Unique Expiries: {self.df['expiry'].nunique()}".ljust(55) + "║")
        print("╠══════════════════════════════════════════════════════════╣")
        print("║ DATA QUALITY FLAGS                                       ║")
        print(f"║ Zero/Null LTP      : {quality_issues['zero_ltp']} rows ({zero_ltp_pct:.1f}%)".ljust(55) + "║")
        print(f"║ Missing IV (dte>0) : {quality_issues['missing_iv']} rows ({missing_iv_pct:.1f}%)".ljust(55) + "║")
        print(f"║ Extreme IV (>200%) : {quality_issues['extreme_iv']} rows ({extreme_iv_pct:.1f}%)".ljust(55) + "║")
        print(f"║ Bad Greeks         : {bad_greeks} rows ({bad_greeks_pct:.1f}%)".ljust(55) + "║")
        print(f"║ Missing Dates >3d  : {len(continuity_data['gaps'])} gaps".ljust(55) + "║")
        print("╠══════════════════════════════════════════════════════════╣")
        print("║ USABILITY VERDICT                                        ║")
        print(f"║ Backtesting Grade  : {grade}".ljust(55) + "║")
        print(f"║ Recommendation     : {verdict[:45]}".ljust(55) + "║")
        print("╚══════════════════════════════════════════════════════════╝")
        
        return {
            'grade': grade,
            'coverage_pct': coverage_pct,
            'zero_ltp_pct': zero_ltp_pct,
            'missing_iv_pct': missing_iv_pct,
            'bad_greeks_pct': bad_greeks_pct,
            'total_flags': total_flags
        }
    
    # ========== SECTION 6: EXPORT FILES ==========
    def section_6_exports(self, output_dir):
        print("\n" + "="*70)
        print("💾 SECTION 6: EXPORTING REPORTS")
        print("="*70)
        
        os.makedirs(output_dir, exist_ok=True)
        
        # 6.1 Quality report summary
        summary = []
        for date, group in self.df.groupby('date'):
            ce_count = len(group[group['optiontype'] == 'CE'])
            pe_count = len(group[group['optiontype'] == 'PE'])
            strikes = len(group['strike'].unique())
            date_flags = [f for f in self.flags_detail if f['date'] == date.strftime('%d-%b-%Y')]
            pcp_violations = len([f for f in date_flags if f['flag_type'] == 'PCP_VIOLATION'])
            missing_iv = len([f for f in date_flags if f['flag_type'] == 'MISSING_IV'])
            zero_ltp = len([f for f in date_flags if f['flag_type'] == 'ZERO_LTP'])
            
            summary.append({
                'trading_date': date.strftime('%d-%b-%Y'),
                'ce_count': ce_count,
                'pe_count': pe_count,
                'strikes_covered': strikes,
                'pcp_violations': pcp_violations,
                'missing_iv_count': missing_iv,
                'zero_ltp_count': zero_ltp,
                'total_flags': len(date_flags)
            })
        
        summary_df = pd.DataFrame(summary)
        summary_path = os.path.join(output_dir, 'quality_report_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        print(f"   ✓ {summary_path}")
        
        # 6.2 Detailed flags
        flags_df = pd.DataFrame(self.flags_detail)
        flags_path = os.path.join(output_dir, 'quality_flags_detail.csv')
        flags_df.to_csv(flags_path, index=False)
        print(f"   ✓ {flags_path} ({len(flags_df):,} flagged rows)")
        
        # 6.3 IV Surface (latest date)
        latest_date = self.df['date'].max()
        latest = self.df[self.df['date'] == latest_date].copy()
        
        if len(latest) > 0:
            latest['dte_bucket'] = pd.cut(latest['dte'], bins=[0, 7, 30, 60, 999], 
                                         labels=['0-7d', '8-30d', '31-60d', '61+d'])
            iv_surface = latest[['strike', 'strikeoffset', 'dte_bucket', 'optiontype', 'iv']].copy()
            iv_path = os.path.join(output_dir, 'iv_surface_latest.csv')
            iv_surface.to_csv(iv_path, index=False)
            print(f"   ✓ {iv_path}")
        
        # 6.4 Coverage by year
        coverage_by_year = self.df.groupby(self.df['date'].dt.year).agg({
            'date': 'nunique',
            'expiry': 'nunique',
            'strike': 'count',
        })
        coverage_by_year.columns = ['Trading Days', 'Unique Expiries', 'Total Rows']
        coverage_by_year['Year'] = coverage_by_year.index
        coverage_by_year['avg_strikes_per_date'] = (coverage_by_year['Total Rows'] / coverage_by_year['Trading Days']).round(1)
        coverage_by_year = coverage_by_year[['Year', 'Trading Days', 'Unique Expiries', 'Total Rows', 'avg_strikes_per_date']].reset_index(drop=True)
        coverage_by_year.columns = ['Year', 'Trading Days', 'Unique Expiries', 'Total Rows', 'Avg Strikes/Date']
        coverage_path = os.path.join(output_dir, 'coverage_by_year.csv')
        coverage_by_year.to_csv(coverage_path, index=False)
        print(f"   ✓ {coverage_path}")
        
        print(f"\n   📂 All reports saved to: {output_dir}")
        
        return {
            'summary': summary_path,
            'flags': flags_path,
            'iv_surface': iv_path if len(latest) > 0 else None,
            'coverage': coverage_path
        }
    
    def run(self, output_dir='./quality_reports/'):
        """Execute complete evaluation"""
        coverage = self.section_1_coverage()
        strikes = self.section_2_strikes()
        quality = self.section_3_field_quality()
        continuity = self.section_4_continuity()
        scorecard = self.section_5_scorecard(coverage, quality, continuity)
        exports = self.section_6_exports(output_dir)
        
        print("\n" + "="*70)
        print("✅ EVALUATION COMPLETE")
        print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='NIFTY Options Chain - Data Quality Evaluator',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input', default='./nse-options-last-5-years/processed_data/nifty_atm_chain.csv',
                       help='Path to nifty_atm_chain.csv')
    parser.add_argument('--output_dir', default='./quality_reports/',
                       help='Output directory for reports')
    
    args = parser.parse_args()
    
    evaluator = ChainDataEvaluator(args.input)
    evaluator.run(args.output_dir)


if __name__ == '__main__':
    main()
