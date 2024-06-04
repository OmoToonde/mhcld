# Databricks notebook source
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr, rand, row_number
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# Assuming the existing Spark session
spark = SparkSession.builder.appName("MHCLD Data - Gold Table").getOrCreate()

# Load the train, validation, and test datasets
train_df = spark.table("mhcld_bronze_train")
val_df = spark.table("mhcld_bronze_val")
test_df = spark.table("mhcld_bronze_test")

# Union the datasets to create a single DataFrame
combined_df = train_df.union(val_df).union(test_df)

# Perform additional transformations and calculations
# Example transformation: Calculate the age group
combined_df = combined_df.withColumn(
    "AGE_GROUP",
    expr("CASE WHEN AGE BETWEEN 0 AND 17 THEN '0-17' " +
         "WHEN AGE BETWEEN 18 AND 34 THEN '18-34' " +
         "WHEN AGE BETWEEN 35 AND 54 THEN '35-54' " +
         "WHEN AGE >= 55 THEN '55+' " +
         "ELSE 'UNKNOWN' END")
)

# Get the schema of the existing table for comparison
existing_table_schema = spark.table("mhcld_bronze_partitioned").schema

# Function to adjust the schema of a DataFrame to match the existing table schema
def adjust_schema(df, target_schema):
    target_field_names = {field.name for field in target_schema}
    for field in df.schema:
        if field.name not in target_field_names:
            df = df.drop(field.name)
    for field in target_schema:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        else:
            df = df.withColumn(field.name, expr("NULL").cast(field.dataType))
    return df

# Adjust the schema of combined_df to match the existing table schema
gold_df = adjust_schema(combined_df, existing_table_schema)

# Function to check if the schema matches the expected schema
def check_schema_match(df_schema, target_schema):
    if len(df_schema) != len(target_schema):
        return False
    for df_field, target_field in zip(df_schema, target_schema):
        if df_field.name != target_field.name or df_field.dataType != target_field.dataType:
            print(f"Mismatch found: {df_field.name} ({df_field.dataType}) != {target_field.name} ({target_field.dataType})")
            return False
    return True

# Check and print schemas
def print_schema_diff(df_schema, target_schema):
    print("Schema differences:")
    df_fields = {field.name: field.dataType for field in df_schema}
    target_fields = {field.name: field.dataType for field in target_schema}
    
    for field_name, data_type in df_fields.items():
        if field_name not in target_fields:
            print(f"Field '{field_name}' is missing in target schema.")
        elif data_type != target_fields[field_name]:
            print(f"Field '{field_name}' has type {data_type}, but expected {target_fields[field_name]}.")
    
    for field_name in target_fields:
        if field_name not in df_fields:
            print(f"Field '{field_name}' is missing in DataFrame schema.")

if not check_schema_match(gold_df.schema, existing_table_schema):
    print_schema_diff(gold_df.schema, existing_table_schema)
    raise ValueError("Gold set schema does not match the expected schema.")

# Add AGE_GROUP column back for partitioning
gold_df = gold_df.withColumn(
    "AGE_GROUP",
    expr("CASE WHEN AGE BETWEEN 0 AND 17 THEN '0-17' " +
         "WHEN AGE BETWEEN 18 AND 34 THEN '18-34' " +
         "WHEN AGE BETWEEN 35 AND 54 THEN '35-54' " +
         "WHEN AGE >= 55 THEN '55+' " +
         "ELSE 'UNKNOWN' END")
)

# Save the final curated dataset as the gold table partitioned by "YEAR" and "AGE_GROUP"
gold_df.write.partitionBy("YEAR", "AGE_GROUP").format("delta").mode("overwrite").saveAsTable("mhcld_gold_partitioned")

print("Data saved to the gold table with partitioning.")


# COMMAND ----------

# MAGIC %md
# MAGIC To monitor the success and failure of the gold layer processing, we could consider implementing the following strategies:
# MAGIC
# MAGIC 1. Logging: Use structured logging to capture key events, errors, and performance metrics throughout the processing pipeline. Ensure logs include timestamps, processing steps, and error details.
# MAGIC
# MAGIC 2. Metrics: Track metrics such as processing time, data volume, error rates, and resource usage. Use dashboards in tools like Grafana or Kibana for real-time monitoring.
# MAGIC
# MAGIC 3. Data Quality Checks: Implement automated checks for data quality, including schema validation, missing values, and consistency checks.
# MAGIC
# MAGIC 4. Retry Mechanisms: Incorporate retry logic for transient failures and log these retries for further analysis.
# MAGIC
# MAGIC 5. Audit Trails: Maintain an audit trail of data processing steps and transformations to ensure traceability and accountability.
# MAGIC
