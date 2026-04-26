# NSE Options Data Scraper & Analyzer

A Python pipeline for downloading, processing, and analyzing historical NSE (National Stock Exchange of India) options data. This tool fetches Bhavcopy files, enriches them with spot prices, interest rates, and earnings data, then calculates implied volatility, Greeks, and other key metrics for quantitative research and strategy development.

> ⚠️ **Important Disclaimer**: This tool is for **educational and research purposes only**. You must comply with NSE's Terms of Use and any relevant regulations (SEBI, etc.) when using this software. The tool does not provide access to NSE data; users must obtain data legally and in accordance with NSE's policies. The authors are not responsible for any misuse or violation of terms.

## Features

- **Data Collection**: Downloads NSE Bhavcopy (daily market data), Yahoo Finance spot prices, FBIL MIFOR interest rates, and earnings calendars.
- **Data Enrichment**: Merges multiple data sources into a unified dataset.
- **Analytics Engine**: Calculates:
  - Implied Volatility (IV) via Black-Scholes + bisection
  - Realized Volatility (Yang-Zhang estimator)
  - Option Greeks (Delta, Gamma, Theta, Vega, Rho)
  - IV Percentile and Rank
  - Put-Call Parity checks
- **Storage Options**:
  - Save enriched data as JSON files
  - Bulk upsert into PostgreSQL (recommended for large datasets)
- **Performance**: Multi-threaded processing for efficient handling of 5+ years of data.
- **Extensibility**: Modular design allows customization of data sources and analytics.

## Project Structure

```
nse-options-data-scraper/
├── bhavcopy/             # NSE Bhavcopy files (ZIPs and extracted CSVs) - **NOT IN REPO**
├── earning_dates/        # Earnings calendar JSONs - **NOT IN REPO**
├── interest_rates/       # FBIL MIFOR CSV files - **NOT IN REPO**
├── yahoo_finance/        # Yahoo Finance spot price JSONs - **NOT IN REPO**
├── processed_data/       # Final merged dataset (JSON or CSV) - **NOT IN REPO**
├── scripts/              # Data download scripts
│   ├── download_bhavcopy.py
│   ├── download_yahoo_data.py
│   ├── download_fbil_rates.py
│   └── download_earnings.py
├── store_in_db/          # PostgreSQL ETL utilities
│   ├── store_s.py        # Multi-threaded ETL driver
│   ├── db_util.py        # Connection pooling and batch upsert
│   └── schema.sql        # Database schema
├── analytics/            # Analysis and calculation modules
│   ├── iv_calculator.py
│   ├── greeks.py
│   ├── volatility.py
│   └── pcp_checker.py
├── utils/                # Helper functions
│   ├── file_handler.py
│   ├── date_utils.py
│   └── logger.py
├── requirements.txt      # Python dependencies
├── .gitignore            # Git ignore rules
└── README.md             # This file
```

## Installation

1. **Clone the repository** (after creating your own repo from this template):
   ```bash
   git clone https://github.com/darshkale/nse-options-data-pipeline.git
   cd nse-options-data-scraper
   ```

2. **Set up a virtual environment** (recommended):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables** (optional but recommended for database credentials):
   - Create a `.env` file in the project root (see `.env.example` if provided) or set environment variables directly.
   - Example variables:
     ```bash
     PG_DSN=postgresql://user:password@localhost:5432/nse_options
     BATCH_SIZE=1000
     MAX_WORKERS=4
     ```

## Usage

### Step 1: Download Data

**Note**: You are responsible for obtaining data legally. This step assumes you have access to NSE Bhavcopy files through legitimate means (e.g., NSE's official data products, authorized vendors, or personal downloads from NSE India website in compliance with their terms).

```bash
# Download NSE Bhavcopy files (requires manual setup of data source)
python scripts/download_bhavcopy.py

# Download Yahoo Finance spot prices (publicly available, no auth required)
python scripts/download_yahoo_data.py

# Download FBIL MIFOR interest rates (publicly available)
python scripts/download_fbil_rates.py

# Download earnings calendar (if using a public or personal source)
python scripts/download_earnings.py
```

### Step 2: Process and Enrich Data

```bash
# Run the full processing pipeline (JSON output)
python process_data.py

# OR, for PostgreSQL storage (recommended for large datasets):
cd store_in_db
python store_s.py
```

### Step 3: Analyze Data

After processing, you can use the enriched data in `processed_data/` for:
- Quantitative research
- Backtesting trading strategies
- Machine learning model training
- Volatility surface construction

## Configuration

- Adjust `scripts/download_bhavcopy.py` to point to your source of Bhavcopy files.
- Modify `store_in_db/schema.sql` if you need custom database columns.
- Set environment variables or edit `.env` for database connection and performance tuning.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Contact

For questions or suggestions, please open an issue in the repository.

---

**Disclaimer again**: This software does not guarantee the accuracy, completeness, or legality of any data obtained through its use. Users are solely responsible for ensuring compliance with all applicable laws, regulations, and terms of service related to financial data usage in their jurisdiction.
