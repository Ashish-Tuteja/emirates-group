####### Data Cleansing
## Description: Remove or correct invalid data entries.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformations").getOrCreate()

# Load CSV data
customer_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\customers.csv")
operations_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\operations.csv")

# Load JSON data
semi_structured_df = spark.read.option("multiline", "true").json(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\cargo_data.json")


cleaned_customer = customer_df.filter(col("Email").contains("@"))
cleaned_operations = operations_df.filter(col("Status").isin(["Completed", "Pending"]))
cleaned_semi_structured = semi_structured_df.filter(col("EventDetails.DetailStatus").isin(["Valid", "Invalid"]))

cleaned_customer.show()
cleaned_operations.show()
cleaned_semi_structured.show()
spark.stop()
