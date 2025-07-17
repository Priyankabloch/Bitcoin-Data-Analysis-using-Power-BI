import pandas as pd
import pyodbc

# Load your CSV
df = pd.read_csv('btc_15m_data_2018_to_2025.csv')
df.columns = df.columns.str.strip()
df['Open time'] = pd.to_datetime(df['Open time'])  # Or whichever date column you want

# Connect to SQL Server LocalDB
conn_str = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=(localdb)\MSSQLLocalDB;"
    r"DATABASE=BitcoinDB;"
    r"Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()








