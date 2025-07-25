from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, year, when, concat_ws
import os

# Create SparkSession
spark = SparkSession.builder \
    .appName("PySpark Orders ETL") \
    .getOrCreate()

# Paths
input_path = "input_data/orders.csv"
output_cleaned = "output_data/cleaned_orders"
output_agg = "output_data/customer_sales_summary"

# Read input CSV
df = spark.read.option("header", True).csv(input_path, inferSchema=True)

# Remove duplicates
df = df.dropDuplicates()

# Handle missing values
df = df.fillna({"City": "Unknown", "Order Amount": 0})

# Convert date and extract month
df = df.withColumn("Order Date", to_date(col("Order Date"), "yyyy-MM-dd"))
df = df.withColumn("Order Month", concat_ws("-", year("Order Date"), month("Order Date")))

# Flag high-value orders
df = df.withColumn("Is High Value", when(col("Order Amount") > 1000, True).otherwise(False))

# Aggregation: Total spend by customer
agg_df = df.groupBy("Customer Name").sum("Order Amount").withColumnRenamed("sum(Order Amount)", "Total Spend")

# Save results
df.write.mode("overwrite").option("header", True).csv(output_cleaned)
agg_df.write.mode("overwrite").option("header", True).csv(output_agg)

print("✅ PySpark ETL completed. Results saved in output_data/")
