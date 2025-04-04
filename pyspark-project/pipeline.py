from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, count, avg

# Initialize Spark
spark = SparkSession.builder \
    .appName("PySpark Data Engineering Project") \
    .getOrCreate()

# Load data
df = spark.read.csv("data/sales_data.csv", header=True, inferSchema=True)

# Clean & transform
df_clean = df.dropna(subset=["Product", "Price", "Quantity", "Date"])
df_clean = df_clean.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
df_transformed = df_clean \
    .withColumn("Year", year(col("Date"))) \
    .withColumn("Month", month(col("Date"))) \
    .withColumn("TotalSale", col("Price") * col("Quantity"))

# Show transformed data
print("=== Transformed Data ===")
df_transformed.show()

# Aggregation
df_agg = df_transformed.groupBy("Product", "Year", "Month") \
    .agg(
        count("*").alias("Total_Transactions"),
        avg("Price").alias("Average_Price"),
        avg("TotalSale").alias("Average_Sale")
    )

# Show aggregated results
print("=== Aggregated Monthly Report ===")
df_agg.show()

spark.stop()
