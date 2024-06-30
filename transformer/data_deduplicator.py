######## 1. Data De-duplication
## Description: Remove duplicate records based on a unique key.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformations").getOrCreate()

# Load CSV data
customer_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\customers.csv")
operations_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\operations.csv")

# Load JSON data
semi_structured_df = spark.read.option("multiline", "true").json(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\cargo_data.json")

# Data De-duplication
customer_dedup = customer_df.dropDuplicates(["CustomerID"])
operations_dedup = operations_df.dropDuplicates(["OperationID"])
semi_structured_dedup = semi_structured_df.dropDuplicates(["WaybillNumber"])

customer_dedup.show()
operations_dedup.show()
semi_structured_dedup.show()
spark.stop()