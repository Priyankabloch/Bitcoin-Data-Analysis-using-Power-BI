import pyodbc

# Replace with your actual credentials and database
conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-O1840IA;"   # or just localhost
    "Database=BitcoinDB;"
    "Trusted_Connection=yes;"           # Use 'yes' for Windows auth, or replace with UID/PWD for SQL auth
)

cursor = conn.cursor()

# Run a test query
cursor.execute("select * from BitcoinPrices")

for row in cursor.fetchall():
    print(row)

conn.close()


