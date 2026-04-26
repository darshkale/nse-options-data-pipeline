from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from typing import Optional, List, Dict
import os

app = FastAPI(
    title="NSE Options Data API",
    description="API for accessing cleaned NIFTY options data with IV, Greeks, and execution metrics",
    version="1.0.0"
)

# Load sample data (in production, this would be replaced with a database connection)
DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'sample_data.csv')

def load_data() -> pd.DataFrame:
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Data file not found at {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    # Convert timestamp if present
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@app.get("/")
async def root():
    return {
        "message": "NSE Options Data API",
        "description": "Access cleaned NIFTY options data with IV, Greeks, and execution metrics",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/chain")
async def get_option_chain(
    date: Optional[str] = None,
    strike: Optional[int] = None,
    option_type: Optional[str] = None
):
    """
    Get option chain with optional filters.
    - date: YYYY-MM-DD format (filters by timestamp date)
    - strike: exact strike price to filter (integer)
    - option_type: 'CE' for call, 'PE' for put
    If no filters provided, returns all available data (or most recent if date not given).
    """
    try:
        df = load_data()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    # Filter by date if provided
    if date:
        try:
            target_date = pd.to_datetime(date).date()
            if 'timestamp' in df.columns:
                df = df[df['timestamp'].dt.date == target_date]
            else:
                # If no timestamp column, return all data (or could filter by another date column)
                pass
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    # Filter by strike if provided
    if strike is not None:
        if 'strike' not in df.columns:
            raise HTTPException(status_code=400, detail="Strike column not available in data")
        df = df[df['strike'] == strike]
    
    # Filter by option_type if provided
    if option_type is not None:
        if 'option_type' not in df.columns:
            raise HTTPException(status_code=400, detail="Option_type column not available in data")
        # Normalize to uppercase
        option_type = option_type.upper()
        if option_type not in ['CE', 'PE']:
            raise HTTPException(status_code=400, detail="Option_type must be 'CE' or 'PE'")
        df = df[df['option_type'] == option_type]
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for the given filters")
    
    # Convert DataFrame to list of dictionaries for JSON response
    # Handle datetime objects
    records = df.to_dict(orient='records')
    for record in records:
        for key, value in record.items():
            if isinstance(value, pd.Timestamp):
                record[key] = value.isoformat()
    
    return {
        "count": len(records),
        "filters": {
            "date": date,
            "strike": strike,
            "option_type": option_type
        },
        "data": records
    }

# Optional: Add health check endpoint
@app.get("/health")
async def health_check():
    try:
        df = load_data()
        return {"status": "healthy", "records_loaded": len(df)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: {str(e)}")