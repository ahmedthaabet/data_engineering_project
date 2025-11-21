import pandas as pd
import numpy as np
from db_utils import save_to_db



daily_trade_prices = pd.read_csv("data/daily_trade_prices.csv")
dim_customer = pd.read_csv("data/dim_customer.csv")
dim_date = pd.read_csv("data/dim_date.csv")
dim_stock = pd.read_csv("data/dim_stock.csv")
trades = pd.read_csv("data/trades.csv")

daily_trade_prices = daily_trade_prices.ffill()

trades['cumulative_portfolio_value_log'] = np.log1p(trades['cumulative_portfolio_value'])
trades.rename(columns={'timestamp': 'trade_date'}, inplace=True)

# --- CONFIG ---
output_path = 'data/final_dataset.csv'

# --- Ensure date types and names ---
trades['trade_date'] = pd.to_datetime(trades['trade_date']).dt.date
daily_trade_prices['date'] = pd.to_datetime(daily_trade_prices['date']).dt.date
dim_date['date'] = pd.to_datetime(dim_date['date']).dt.date

# --- 1) Wide -> Long for daily prices ---
price_long = pd.melt(
    daily_trade_prices,
    id_vars=['date'],
    var_name='stock_ticker',
    value_name='stock_price'
)
price_long['stock_ticker'] = price_long['stock_ticker'].astype(str).str.strip()

# --- 2) Merge trades with price on (trade_date, stock_ticker) ---
merged = trades.merge(
    price_long,
    left_on=['trade_date', 'stock_ticker'],
    right_on=['date', 'stock_ticker'],
    how='left',
    validate='m:1'
)

# Drop the redundant 'date' from price_long keep trade_date as canonical
if 'date' in merged.columns:
    merged = merged.drop(columns=['date'])

# --- 3) Merge customer attributes (account type) ---
dim_customer_ren = dim_customer.rename(columns={'account_type': 'customer_account_type'})
merged = merged.merge(
    dim_customer_ren[['customer_id', 'customer_account_type']],
    on='customer_id',
    how='left',
    validate='m:1'
)

# --- 4) Merge date attributes from dim_date (day_name, is_weekend, is_holiday) ---
# dim_date has column 'date' which we will join to trades.trade_date
merged = merged.merge(
    dim_date[['date', 'day_name', 'is_weekend', 'is_holiday']],
    left_on='trade_date',
    right_on='date',
    how='left',
    validate='m:1'
)

# drop the joined 'date' (we keep trade_date)
if 'date' in merged.columns:
    merged = merged.drop(columns=['date'])

# --- 5) Merge stock metadata ---
dim_stock_ren = dim_stock.rename(columns={
    'liquidity_tier': 'stock_liquidity_tier',
    'sector': 'stock_sector',
    'industry': 'stock_industry'
})
merged = merged.merge(
    dim_stock_ren[['stock_ticker', 'stock_liquidity_tier', 'stock_sector', 'stock_industry']],
    on='stock_ticker',
    how='left',
    validate='m:1'
)

# --- 6) Ensure numeric and compute total_trade_amount ---
merged['stock_price'] = pd.to_numeric(merged['stock_price'], errors='coerce')
merged['quantity'] = pd.to_numeric(merged['quantity'], errors='coerce')
merged['total_trade_amount'] = merged['stock_price'] * merged['quantity']

# --- 7) Final columns in required order ---
final_cols = [
    'transaction_id',
    'trade_date',
    'customer_id',
    'stock_ticker',
    'transaction_type',
    'quantity',
    'average_trade_size',
    'stock_price',
    'total_trade_amount',
    'customer_account_type',
    'day_name',
    'is_weekend',
    'is_holiday',
    'stock_liquidity_tier',
    'stock_sector',
    'stock_industry'
]

# Check presence of expected columns
missing = [c for c in final_cols if c not in merged.columns]
if missing:
    msg = f"ERROR: missing expected columns after merges: {missing}"

    # fail loudly so user can fix upstream problem (typo, rename, etc.)
    raise KeyError(msg)

# Select and reorder columns exactly as requested
final_df = merged[final_cols].copy()

# Reset index to clean 0..N-1
final_df = final_df.reset_index(drop=True)
save_to_db(final_df, 'milestone1_cleaned_dataset')