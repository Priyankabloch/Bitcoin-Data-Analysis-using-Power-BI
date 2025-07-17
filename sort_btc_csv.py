import pandas as pd

# Read the CSV file
df = pd.read_csv('actual_vs_predicted_btc_close.csv')

# Convert 'Open time' to datetime format
df['Open time'] = pd.to_datetime(df['Open time'], format='%m/%d/%Y %H:%M')

# Sort the dataframe by 'Open time' in ascending order
df_sorted = df.sort_values(by='Open time')

# Save the sorted dataframe to a new CSV file
df_sorted.to_csv('sorted_actual_vs_predicted_btc_close.csv', index=False)

print("CSV file has been sorted by date and saved as 'sorted_actual_vs_predicted_btc_close.csv'.")