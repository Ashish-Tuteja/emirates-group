###### 3. Schema Enforcement
#### Description: Ensure data conforms to a predefined schema.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformations").getOrCreate()

# Load JSON data
semi_structured_df = spark.read.option("multiline", "true").json(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\cargo_data.json")


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define schemas
customer_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("ContactNumber", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Address", StringType(), True)
])

operations_schema = StructType([
    StructField("OperationID", StringType(), True),
    StructField("WaybillNumber", IntegerType(), True),
    StructField("OperationType", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Timestamp", TimestampType(), True)
])

# Apply schemas
customer_df = spark.read.schema(customer_schema).csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\customers.csv")
operations_df = spark.read.schema(operations_schema).csv(r"C:\Users\Lenovo\PycharmProjects\Sample\source_data\operations.csv")

customer_df.printSchema()
operations_df.printSchema()
spark.stop()