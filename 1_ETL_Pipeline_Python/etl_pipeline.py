import pandas as pd
from datetime import datetime
import os

# Create input/output folders 
os.makedirs("input_data", exist_ok=True)
os.makedirs("output_data", exist_ok=True)

#  Read input CSV file
input_path = "input_data/orders.csv"
df = pd.read_csv(input_path)

# Clean data - drop duplicates, handle missing values
df.drop_duplicates(inplace=True)
df.fillna({"City": "Unknown", "Order Amount": 0}, inplace=True)

# Feature Engineering - Add order month & flag high-value orders
df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
df['Order Month'] = df['Order Date'].dt.strftime('%Y-%m')
df['Is High Value'] = df['Order Amount'] > 1000

# Aggregation - Total sales per customer
agg_df = df.groupby("Customer Name")['Order Amount'].sum().reset_index()
agg_df.rename(columns={"Order Amount": "Total Spend"}, inplace=True)

# Save transformed outputs
df.to_csv("output_data/cleaned_orders.csv", index=False)
agg_df.to_csv("output_data/customer_sales_summary.csv", index=False)

print("✅ ETL Process completed. Output files saved in 'output_data/' folder.")
