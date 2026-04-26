# NSE Options Data Pipeline Architecture

## Overview
This document describes the architecture of the NSE Options Data Pipeline, which transforms raw NSE Bhavcopy data into a enriched dataset suitable for quantitative research and backtesting.

## Components

### 1. Data Acquisition Layer
Responsible for downloading raw data from various sources.

```
+------------------+     +------------------+     +------------------+
|  NSE Bhavcopy    |     |  Yahoo Finance   |     |  FBIL MIFOR      |
|  (Manual/Legacy) |     |  Spot Prices     |     |  Interest Rates  |
+------------------+     +------------------+     +------------------+
         \                       /                     /
          \                     /                     /
           \                   /                     /
            \                 /                     /
             \               /                     /
              \             /                     /
               \           /                     /
                \         /                     /
                 \       /                     /
                  \     /                     /
                   \   /                     /
                    \ /                     /
+------------------v------------------v------------------v+
|                        Data Ingestion Service          |
|  - download_bhavcopy.py                                |
|  - download_yahoo_data.py                              |
|  - download_fbil_rates.py                              |
|  - download_earnings.py                                |
+------------------+------------------+------------------+
                   |                   |                   |
                   v                   v                   v
+------------------+------------------+------------------+
|     Raw Data Storage (Tier 0)                           |
|  - bhavcopy/ (ZIPs/CSVs)                               |
|  - yahoo_finance/ (JSON)                               |
|  - interest_rates/ (CSV)                               |
|  - earning_dates/ (JSON)                               |
+------------------+------------------+------------------+
```

### 2. Data Processing Layer
Cleans, enriches, and calculates derived metrics.

```
+------------------+     +------------------+     +------------------+
|   Raw Data       |     |  Enrichment      |     |  Greeks & IV     |
|  (Tier 0)        |     |  Module          |     |  Calculation     |
+------------------+     +------------------+     +------------------+
         \                       /                     /
          \                     /                     /
           \                   /                     /
            \                 /                     /
             \               /                     /
              \             /                     /
               \           /                     /
                \         /                     /
                 \       /                     /
                  \     /                     /
                   \   /                     /
                    \ /                     /
+------------------v------------------v------------------v+
|                     Processing Engine                   |
|  - process_data.py                                      |
|  - iv_calculator.py                                     |
|  - greeks.py                                            |
|  - volatility.py                                        |
|  - pcp_checker.py                                       |
+------------------+------------------+------------------+
                   |                   |                   |
                   v                   v                   v
+------------------+------------------+------------------+
|   Enriched Data Storage (Tier 1)                         |
|  - processed_data/ (JSON or CSV)                        |
+------------------+------------------+------------------+
```

### 3. Storage & Query Layer
Provides efficient access to the enriched dataset.

```
+------------------+     +------------------+
|   JSON Files     |     |  PostgreSQL DB   |
|  (for small sets)|     |  (recommended for|
|                  |     |   large datasets)|
+------------------+     +------------------+
         \                       /
          \                     /
           \                   /
            \                 /
             \               /
              \             /
               \           /
                \         /
                 \       /
                  \     /
                   \   /
                    \ /
           +------------------v------------------+
           |         Data Access API             |
           |  - FastAPI (api/main.py)           |
           |  - Endpoints:                      |
           |    • GET /                         |
           |    • GET /chain?date=YYYY-MM-DD    |
           +------------------+------------------+
                                  |
                                  v
                         +------------------+
                         |   Consumer Apps  |
                         |  - Notebooks     |
                         |  - Backtesters   |
                         |  - ML Models     |
                         +------------------+
```

### 4. Execution Modeling Layer
Adds realism to the dataset for backtesting.

```
+------------------+
|  Enriched Data   |
|  (IV, Greeks, etc)|
+------------------+
          |
          v
+------------------+
|  Execution Model |
|  - Bid-ask spread|
|  - Slippage      |
|  - Market impact |
|  - Liquidity filter|
+------------------+
          |
          v
+------------------+
|  Tradability     |
|  Filtering       |
|  - Volume/OI     |
|  - IV rank       |
+------------------+
          |
          v
+------------------+
|  Final Dataset   |
|  Ready for       |
|  backtesting     |
+------------------+
```

## Key Calculations

### Implied Volatility (IV)
- Uses Black-Scholes formula with bisection method.
- Inputs: option price (mid), strike, time to expiry, risk-free rate, underlying price.
- Output: IV that matches the market price.

### Option Greeks
- Analytic formulas from Black-Scholes:
  - Delta: N(d1) for calls, -N(-d1) for puts
  - Gamma: N'(d1) / (S * sigma * sqrt(T))
  - Vega: S * sqrt(T) * N'(d1)
  - Theta: -(S * sigma * N'(d1)) / (2 * sqrt(T)) - r*K*e^(-rT)*N(d2) (call)
  - Rho: K*T*e^(-rT)*N(d2) (call)

### Volatility Measures
- Realized Volatility: Yang-Zhang estimator using OHLC data.
- IV Percentile/Rank: Historical IV comparison.

### Execution Costs
- Bid-ask spread: (ask - bid) / mid
- Slippage model: percentage of spread based on order size and liquidity.
- Market impact: temporary and permanent impact components.

## API Layer
Built with FastAPI for high performance and automatic API documentation.

Endpoints:
- GET /: API information
- GET /chain?date=YYYY-MM-DD: Retrieve option chain for a given date
- GET /health: Health check

## Data Flow Summary
1. Raw data downloaded manually or via scripts into respective folders.
2. Processing engine reads raw data, enriches with external data, calculates IV/Greeks.
3. Enriched data stored as JSON/CSV (Tier 1) or loaded into PostgreSQL.
4. Execution modeling and tradability filters applied to produce final dataset.
5. API serves the final dataset to consumers (notebooks, backtesters, ML models).
6. Users can run the demo notebook or build their own strategies.

## Dependencies
- Core: pandas, numpy, requests
- Optional: psycopg2-binary (for PostgreSQL), fastapi, uvicorn
- See requirements.txt for full list.

## Extensibility
- Add new data sources by creating a download script and modifying process_data.py.
- Plug in alternative IV models (e.g., machine learning based) in iv_calculator.py.
- Extend execution model with more sophisticated slippage models.