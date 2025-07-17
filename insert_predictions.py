import pandas as pd
import pyodbc

# Load the CSV file
csv_path = r'D:\Bitcoin_Project_2025\Bitcoin 1 day Cleaned Data..and latest.csv'

try:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()  # Strip leading/trailing whitespace
    print("✅ CSV loaded. Columns:", df.columns.tolist())
except Exception as e:
    print(f"❌ Failed to load CSV: {e}")
    exit()

# Rename columns to match required names
df.rename(columns={
    'Open time': 'Date',            # Assuming 'Open time' is your date column
    'Close': 'Actual_Close',        # Assuming 'Close' is your Actual Close
    'MA_7': 'Predicted_Close'       # Assuming 'MA_7' is your Predicted Close
}, inplace=True)

# Check required columns
required_cols = ['Date', 'Actual_Close', 'Predicted_Close']
if not all(col in df.columns for col in required_cols):
    print(f"❌ Required columns missing! Found columns: {df.columns.tolist()}")
    exit()

# Convert date and ensure correct types
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
df['Actual_Close'] = df['Actual_Close'].astype(float)
df['Predicted_Close'] = df['Predicted_Close'].astype(float)

# Connect to SQL Server
try:
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=DESKTOP-O1840IA;"  # Change if needed
        "Database=BitcoinDB;"      # Your database name
        "Trusted_Connection=yes;"
    )
except pyodbc.Error as e:
    print(f"❌ Database connection failed: {e}")
    exit()

cursor = conn.cursor()

# Drop the existing table (if it exists)
try:
    cursor.execute("IF OBJECT_ID('BitcoinPredictions', 'U') IS NOT NULL DROP TABLE BitcoinPredictions")
    conn.commit()
    print("✅ Existing BitcoinPredictions table dropped.")
except pyodbc.Error as e:
    print(f"⚠️ Warning: Could not drop table: {e}")

# Create the new table
try:
    cursor.execute("""
    CREATE TABLE BitcoinPredictions (
        Date DATE NOT NULL,
        Actual_Close FLOAT NOT NULL,
        Predicted_Close FLOAT NOT NULL
    )
    """)
    conn.commit()
    print("✅ New BitcoinPredictions table created.")
except pyodbc.Error as e:
    print(f"❌ Failed to create table: {e}")
    cursor.close()
    conn.close()
    exit()

# Insert data
try:
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO BitcoinPredictions (Date, Actual_Close, Predicted_Close) VALUES (?, ?, ?)",
            row['Date'], row['Actual_Close'], row['Predicted_Close']
        )
    conn.commit()
    print("✅ Predictive data inserted successfully.")
except pyodbc.Error as e:
    print(f"❌ Failed to insert data: {e}")

# Close connection
cursor.close()
conn.close()

