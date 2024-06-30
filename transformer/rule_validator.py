#### 4. Business Rules Validation
### Description: Validate data against business rules.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformations").getOrCreate()

# Load CSV data
customer_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\customers.csv")
operations_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\operations.csv")

# Load JSON data
semi_structured_df = spark.read.option("multiline", "true").json(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\cargo_data.json")


# Business Rules Validation
valid_customers = customer_df.filter(col("ContactNumber").rlike(r'^\d{10}$'))
valid_operations = operations_df.filter(col("WaybillNumber").isNotNull() & col("Timestamp").isNotNull())
valid_semi_structured = semi_structured_df.filter(col("EventDetails.DetailID").isNotNull())

valid_customers.show()
valid_operations.show()
valid_semi_structured.show()
spark.stop()