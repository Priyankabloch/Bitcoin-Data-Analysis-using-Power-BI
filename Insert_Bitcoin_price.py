import pandas as pd
import pyodbc
from datetime import datetime

# ✅ Use raw string to fix invalid escape sequence issue
df = pd.read_csv(r'D:\Bitcoin_Project_2025\final_bitcoin_data.csv')

# ✅ Rename columns to match SQL Server table
df.rename(columns={
    'Open time': 'Date',
    'Close': 'ClosePrice',
    'Open': 'OpenPrice',
    'High': 'HighPrice',
    'Low': 'LowPrice',
    'Volume': 'Volume',
    'Close time': 'CloseTime',
    'Quote asset volume': 'QuoteAssetVolume',
    'Number of trades': 'NumberOfTrades',
    'Taker buy base asset volume': 'TakerBuyBaseAssetVolume',
    'Taker buy quote asset volume': 'TakerBuyQuoteAssetVolume',
    'Close_Lag1': 'CloseLag1',
    'Close_Lag7': 'CloseLag7',
    'MA_7': 'MovingAverage7',
    'MA_30': 'MovingAverage30',
    'Volatility_7': 'Volatility7',
    'Price_Change': 'PriceChange',
    'Return': 'PriceReturn',
    'DayOfWeek': 'DayOfWeek',
    'Month': 'Month',
    'Price_Change_Direction': 'PriceChangeDirection'
}, inplace=True)

# ✅ Convert columns to appropriate types
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

def parse_close_time(time_str):
    if pd.isna(time_str) or not isinstance(time_str, str):
        return None
    try:
        time_obj = pd.to_datetime(time_str, format='%H:%M.%S', errors='coerce')
        if pd.isna(time_obj):
            return None
        return datetime(2000, 1, 1, time_obj.hour, time_obj.minute, time_obj.second)
    except Exception as e:
        print(f"Failed to parse CloseTime '{time_str}': {e}")
        return None

df['CloseTime'] = df['CloseTime'].apply(parse_close_time)

# ✅ Check and clean data
print("Checking for NaN/None values in critical columns:")
print(df[['Date', 'CloseTime', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice', 'NumberOfTrades']].isna().sum())

invalid_close_time = df['CloseTime'].isna()
if invalid_close_time.any():
    print("Rows with invalid CloseTime values:")
    print(df[invalid_close_time][['Date', 'CloseTime', 'ClosePrice']])
    df = df[~invalid_close_time]
    print(f"Dropped {invalid_close_time.sum()} rows with invalid CloseTime.")

float_cols = [
    'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice', 'Volume', 
    'QuoteAssetVolume', 'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume',
    'CloseLag1', 'CloseLag7', 'MovingAverage7', 'MovingAverage30', 
    'Volatility7', 'PriceChange', 'PriceReturn', 'DayOfWeek', 'Month', 
    'PriceChangeDirection'
]
for col in float_cols:
    df[col] = df[col].replace([float('nan'), float('inf'), -float('inf')], None)

# Cast NumberOfTrades safely
df['NumberOfTrades'] = pd.to_numeric(df['NumberOfTrades'], errors='coerce').fillna(0).astype(int)

# Drop 'Ignore' if it exists
if 'Ignore' in df.columns:
    df.drop(columns=['Ignore'], inplace=True)

# ✅ Connect to SQL Server
try:
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=DESKTOP-O1840IA;"  # Replace with your server name
        "Database=BitcoinDB;"
        "Trusted_Connection=yes;"
    )
    cursor = conn.cursor()

    # ✅ Ensure table exists
    cursor.execute("SELECT 1 FROM sys.tables WHERE name = 'BitcoinDailyPrices'")
    if not cursor.fetchone():
        raise Exception("Table 'BitcoinDailyPrices' does not exist. Please create it first.")

    # ✅ Insert data row by row (batch insert can be added for large data)
    insert_query = """
    INSERT INTO dbo.BitcoinDailyPrices (
        Date, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, CloseTime,
        QuoteAssetVolume, NumberOfTrades, TakerBuyBaseAssetVolume, 
        TakerBuyQuoteAssetVolume, CloseLag1, CloseLag7, MovingAverage7, 
        MovingAverage30, Volatility7, PriceChange, PriceReturn, 
        DayOfWeek, Month, PriceChangeDirection
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for index, row in df.iterrows():
        cursor.execute(insert_query, (
            row['Date'], row['OpenPrice'], row['HighPrice'], row['LowPrice'],
            row['ClosePrice'], row['Volume'], row['CloseTime'],
            row['QuoteAssetVolume'], row['NumberOfTrades'],
            row['TakerBuyBaseAssetVolume'], row['TakerBuyQuoteAssetVolume'],
            row['CloseLag1'], row['CloseLag7'], row['MovingAverage7'],
            row['MovingAverage30'], row['Volatility7'], row['PriceChange'],
            row['PriceReturn'], row['DayOfWeek'], row['Month'],
            row['PriceChangeDirection']
        ))

    conn.commit()
    print(f"✅ Data inserted successfully into BitcoinDailyPrices table.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'conn' in locals():
        conn.close()


