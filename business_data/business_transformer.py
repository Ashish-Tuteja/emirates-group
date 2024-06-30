### Business KPIs and Metrices

## 1. On-Time Delivery Rate
## Description: Percentage of shipments delivered on or before the scheduled delivery date.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark Session
spark = SparkSession.builder.appName("KPI Calculation").getOrCreate()

# Load CSV data
shipments_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\transformed_data\shipments.csv")

# Register DataFrame as Temp View
shipments_df.createOrReplaceTempView("shipments")

# Calculate On-Time Delivery Rate
on_time_delivery_rate = spark.sql("""
    SELECT 
        (SUM(CASE WHEN DeliveryDate <= ScheduledDeliveryDate THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS on_time_delivery_rate
    FROM shipments
""")

on_time_delivery_rate.show()


## 2. Load Factor
## Description: Ratio of cargo space used to total available cargo space.

# Calculate Load Factor
load_factor = spark.sql("""
    SELECT 
        (SUM(TotalWeight) / SUM(AvailableSpace)) * 100 AS load_factor
    FROM shipments
""")

load_factor.show()

## 3. Revenue per Kilogram
## Description: Total revenue generated divided by the total weight of shipments.

# Calculate Revenue per Kilogram
revenue_per_kg = spark.sql("""
    SELECT 
        SUM(TotalRevenue) / SUM(TotalWeight) AS revenue_per_kg
    FROM shipments
""")

revenue_per_kg.show()

## 4. Customer Satisfaction Score
## Description: A metric derived from customer feedback and surveys.

# Load CSV data
customer_feedback_df = spark.read.option("header", "true").csv(r"C:\Users\Lenovo\PycharmProjects\Sample\transformed_data\customer_feedback.csv")

# Register DataFrame as Temp View
customer_feedback_df.createOrReplaceTempView("customer_feedback")

# Calculate Customer Satisfaction Score
customer_satisfaction_score = spark.sql("""
    SELECT 
        AVG(FeedbackScore) AS customer_satisfaction_score
    FROM customer_feedback
""")

customer_satisfaction_score.show()

## 5. Data Accuracy Rate
## Description: Percentage of data entries that are correct and accurate.

# Calculate Data Accuracy Rate
data_accuracy_rate = spark.sql("""
    SELECT 
        (SUM(CASE WHEN RecordStatus = 'Accurate' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS data_accuracy_rate
    FROM customer_feedback
""")

data_accuracy_rate.show()

## 6. Incident Response Time
## Description: Average time taken to respond to and resolve security or compliance incidents.

# Calculate Incident Response Time
incident_response_time = spark.sql("""
    SELECT 
        AVG(TimeToResolve) AS incident_response_time
    FROM customer_feedback
""")

incident_response_time.show()
spark.stop()

