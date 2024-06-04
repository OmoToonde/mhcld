# Databricks notebook source
# MAGIC %md
# MAGIC Initialize Spark and Load DataFrames

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr, rand, row_number
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Spark session
spark = SparkSession.builder.appName("MHCLD Data - Gold Table").getOrCreate()

try:
    # Load the train, validation, and test datasets
    train_df = spark.table("mhcld_bronze_train")
    val_df = spark.table("mhcld_bronze_val")
    test_df = spark.table("mhcld_bronze_test")
    logger.info("Loaded train, validation, and test datasets.")
    
    # Union the datasets to create a single DataFrame
    combined_df = train_df.union(val_df).union(test_df)
    logger.info("Combined train, validation, and test datasets.")

except Exception as e:
    logger.error(f"An error occurred while loading and combining datasets: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Perform Additional Transformations

# COMMAND ----------

try:
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
    logger.info("Performed additional transformations on the combined dataset.")

except Exception as e:
    logger.error(f"An error occurred during additional transformations: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Adjust Schema to Match Existing Table

# COMMAND ----------

try:
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
    logger.info("Adjusted the schema of the combined DataFrame to match the existing table schema.")

except Exception as e:
    logger.error(f"An error occurred during schema adjustment: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Perform Schema Checks
# MAGIC

# COMMAND ----------

try:
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
    logger.info("Schema check passed successfully.")

except Exception as e:
    logger.error(f"An error occurred during schema check: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Save the Final Curated Dataset as Gold Table

# COMMAND ----------

try:
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
    logger.info("Data saved to the gold table with partitioning.")

except Exception as e:
    logger.error(f"An error occurred while saving the gold table: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Verify the Saved Data

# COMMAND ----------

# Function to check if a table exists and its row count
def check_table(table_name):
    if spark._jsparkSession.catalog().tableExists(table_name):
        count = spark.table(table_name).count()
        print(f"Table '{table_name}' exists and contains {count} rows.")
    else:
        print(f"Table '{table_name}' does not exist.")

# Verify the gold table
check_table("mhcld_gold_partitioned")

