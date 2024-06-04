# Databricks notebook source
# MAGIC %md
# MAGIC Load and Verify Bronze Table Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, min, max, mean, lit
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rand
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField
import logging


# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Create a Spark session
    spark = SparkSession.builder.appName("MHCLD Data - Verify Bronze Data").getOrCreate()

    # Load the bronze partitioned table
    bronze_df = spark.table("mhcld_bronze_partitioned")
    logger.info("Loaded bronze partitioned table.")

    # Display the first few rows of the raw data to ensure it contains data
    bronze_df.show(10)  # Displaying first 10 rows for brevity
    logger.info(f"Row count of bronze table: {bronze_df.count()}")

except Exception as e:
    logger.error(f"An error occurred while verifying bronze data: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Data Type Conversion and Filtering CASEID

# COMMAND ----------

try:
    # Ensure CASEID is stored as an integer data type without filtering negative values
    silver_df = bronze_df.withColumn("CASEID", col("CASEID").cast(IntegerType()))

    # Display the first few rows to verify CASEID transformation
    silver_df.show(10)
    logger.info("Displayed the first few rows after CASEID transformation.")

    # Check the row count after CASEID transformation
    row_count = silver_df.count()
    logger.info(f"Row count after CASEID transformation: {row_count}")

except Exception as e:
    logger.error(f"An error occurred during CASEID transformation: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Applying Mappings

# COMMAND ----------

# Define mapping dictionaries
gender_mapping = {1: "Male", 2: "Female", -1: "Unknown"}
race_mapping = {1: "White", 2: "Black", 3: "Asian", 4: "Other", -1: "Unknown"}
ethnicity_mapping = {1: "Hispanic", 2: "Non-Hispanic", -1: "Unknown"}
marital_mapping = {1: "Single", 2: "Married", 3: "Divorced", 4: "Widowed", -1: "Unknown"}
employ_mapping = {1: "Employed", 2: "Unemployed", 3: "Not in Labor Force", -1: "Unknown"}

# Function to apply mapping
def map_column(column, mapping):
    return when(col(column) == -1, "Unknown") \
            .when(col(column) == 1, mapping[1]) \
            .when(col(column) == 2, mapping[2]) \
            .when(col(column) == 3, mapping.get(3, "Other")) \
            .when(col(column) == 4, mapping.get(4, "Other")) \
            .otherwise("Unknown")

try:
    # Apply mappings to convert numeric values to strings
    silver_df = silver_df.withColumn("GENDER", map_column("GENDER", gender_mapping)) \
                         .withColumn("RACE", map_column("RACE", race_mapping)) \
                         .withColumn("ETHNIC", map_column("ETHNIC", ethnicity_mapping)) \
                         .withColumn("MARSTAT", map_column("MARSTAT", marital_mapping)) \
                         .withColumn("EMPLOY", map_column("EMPLOY", employ_mapping))

    # Display the first few rows to verify mappings
    silver_df.show(10)
    logger.info("Displayed the first few rows after applying mappings.")

    # Check the row count after applying mappings
    row_count = silver_df.count()
    logger.info(f"Row count after applying mappings: {row_count}")

except Exception as e:
    logger.error(f"An error occurred during applying mappings: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Creating New Columns

# COMMAND ----------

try:
    # Create new columns for MENHLTH, PHYHLTH, and POORHLTH
    mental_health_conditions = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]  # List of codes representing mental health conditions

    silver_df = silver_df.withColumn(
        "MENHLTH",
        when(
            col("MH1").isin(mental_health_conditions) | col("MH2").isin(mental_health_conditions) | col("MH3").isin(mental_health_conditions),
            1.0
        ).otherwise(0.0)
    )

    silver_df = silver_df.withColumn(
        "PHYHLTH",
        when(col("SUB").isNotNull() | (col("SAP") == 1), 1.0)
        .otherwise(0.0)
    )

    silver_df = silver_df.withColumn(
        "POORHLTH",
        when(col("NUMMHS") > 1, 1.0)
        .otherwise(0.0)
    )

    # Convert MENHLTH, PHYHLTH, and POORHLTH to float data types
    silver_df = silver_df.withColumn("MENHLTH", col("MENHLTH").cast(FloatType())) \
                         .withColumn("PHYHLTH", col("PHYHLTH").cast(FloatType())) \
                         .withColumn("POORHLTH", col("POORHLTH").cast(FloatType()))
    logger.info("Calculated and converted MENHLTH, PHYHLTH, and POORHLTH to float types.")

    # Display the first few rows to verify new columns
    silver_df.show(10)
    logger.info("Displayed the first few rows after creating new columns MENHLTH, PHYHLTH, and POORHLTH.")

    # Check the row count after creating new columns
    row_count = silver_df.count()
    logger.info(f"Row count after creating new columns: {row_count}")

    # Validate the data types for all variables
    silver_df.printSchema()
    display(silver_df)

except Exception as e:
    logger.error(f"An error occurred during creating new columns and converting types: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Data Normalization and Standardization with Logging

# COMMAND ----------

from pyspark.sql.functions import min, max, mean, lit
from pyspark.sql.types import FloatType

try:
    # Fill null values in PHYHLTH with the column mean
    phyhlth_mean = silver_df.agg(mean("PHYHLTH")).collect()[0][0]
    if phyhlth_mean is None:
        phyhlth_mean = 0
    silver_df = silver_df.fillna({"PHYHLTH": phyhlth_mean})
    logger.info("Filled null values in PHYHLTH with the column mean.")

    # Define a function to perform min-max scaling
    def min_max_scaling(df, column):
        min_val = df.agg(min(column)).collect()[0][0]
        max_val = df.agg(max(column)).collect()[0][0]
        if min_val == max_val:
            # If the column is constant, return a column of zeros (normalized value for constant columns)
            return lit(0.0)
        else:
            return (col(column) - min_val) / (max_val - min_val)

    # Normalize MENHLTH, PHYHLTH, POORHLTH and store in new columns
    silver_df = silver_df.withColumn("MENHLTH_NORM", min_max_scaling(silver_df, "MENHLTH").cast(FloatType())) \
                         .withColumn("PHYHLTH_NORM", min_max_scaling(silver_df, "PHYHLTH").cast(FloatType())) \
                         .withColumn("POORHLTH_NORM", min_max_scaling(silver_df, "POORHLTH").cast(FloatType()))
    logger.info("Normalized MENHLTH, PHYHLTH, and POORHLTH using min-max scaling.")

    # Display the first few rows to verify normalization
    silver_df.show(10)
    logger.info("Displayed the first few rows after normalization.")

except Exception as e:
    logger.error(f"An error occurred during data normalization: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Data Partitioning and Sampling with Logging

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, rand
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

try:
    # Select the necessary columns for splitting
    selected_columns = ["GENDER", "RACE", "ETHNIC", "MARSTAT", "EMPLOY"]

    # Assign a random number to each row
    silver_df = silver_df.withColumn("rand", rand())

    # Create a window specification to partition by the demographic variables and order by the random number
    window_spec = Window.partitionBy(*selected_columns).orderBy("rand")

    # Add a row number within each partition
    silver_df = silver_df.withColumn("row_number", row_number().over(window_spec))

    # Calculate the number of rows in each partition
    counts = silver_df.groupBy(*selected_columns).count().collect()
    counts_dict = {(row['GENDER'], row['RACE'], row['ETHNIC'], row['MARSTAT'], row['EMPLOY']): row['count'] for row in counts}

    # Function to calculate the split indices
    def calculate_split_indices(count, train_ratio=0.7, val_ratio=0.2):
        train_end = int(count * train_ratio)
        val_end = train_end + int(count * val_ratio)
        return train_end, val_end

    # Define UDF to assign splits
    @F.udf(StringType())
    def assign_split(row_number, gender, race, ethnic, marstat, employ):
        count = counts_dict.get((gender, race, ethnic, marstat, employ), 0)
        if count == 0:
            return 'test'
        train_end, val_end = calculate_split_indices(count)
        if row_number <= train_end:
            return 'train'
        elif row_number <= val_end:
            return 'val'
        else:
            return 'test'

    # Add the split column
    silver_df = silver_df.withColumn("split", assign_split(col("row_number"), col("GENDER"), col("RACE"), col("ETHNIC"), col("MARSTAT"), col("EMPLOY")))

    # Separate the dataset into training, validation, and testing sets
    train_df = silver_df.filter(col("split") == 'train').drop("rand", "row_number", "split")
    val_df = silver_df.filter(col("split") == 'val').drop("rand", "row_number", "split")
    test_df = silver_df.filter(col("split") == 'test').drop("rand", "row_number", "split")

    logger.info("Separated the dataset into training, validation, and testing sets.")

except Exception as e:
    logger.error(f"An error occurred during data partitioning and sampling: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC  Schema Checks and Adjustments with Logging

# COMMAND ----------

# Drop existing tables if they exist
spark.sql("DROP TABLE IF EXISTS mhcld_bronze_train")
spark.sql("DROP TABLE IF EXISTS mhcld_bronze_val")
spark.sql("DROP TABLE IF EXISTS mhcld_bronze_test")

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
            raise ValueError(f"Column '{field.name}' is missing in the DataFrame.")
    return df

# Adjust the schema of train_df, val_df, and test_df to match the existing table schema
try:
    train_df = adjust_schema(train_df, existing_table_schema)
    val_df = adjust_schema(val_df, existing_table_schema)
    test_df = adjust_schema(test_df, existing_table_schema)
    logger.info("Adjusted the schema of the training, validation, and testing sets.")

except Exception as e:
    logger.error(f"An error occurred during schema adjustment: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Save the DataFrames as Tables

# COMMAND ----------

try:
    # Save the training, validation, and testing sets as tables without partitioning
    train_df.write.format("delta").mode("overwrite").saveAsTable("mhcld_bronze_train")
    val_df.write.format("delta").mode("overwrite").saveAsTable("mhcld_bronze_val")
    test_df.write.format("delta").mode("overwrite").saveAsTable("mhcld_bronze_test")
    logger.info("Saved the training, validation, and testing sets as Delta tables.")

except Exception as e:
    logger.error(f"An error occurred while saving the DataFrames as tables: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Drop Unnecessary Columns and Save Silver Table

# COMMAND ----------

try:
    # Drop unnecessary columns from silver_df
    silver_df = silver_df.drop("rand", "row_number", "split")

    # Adjust the schema of silver_df to match the existing table schema
    silver_df = adjust_schema(silver_df, existing_table_schema)
    logger.info("Adjusted the schema of the silver DataFrame to match the existing table schema.")

    # Partition the silver_df DataFrame by "YEAR" and save it to the silver table
    silver_df.write.partitionBy("YEAR").format("delta").mode("overwrite").saveAsTable("mhcld_silver_partitioned")
    logger.info("Data saved to the silver table with partitioning.")

except Exception as e:
    logger.error(f"An error occurred during final adjustments and saving the silver table: {str(e)}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC Verify Saved Data

# COMMAND ----------

# List of table names to verify
tables = ["mhcld_bronze_train", "mhcld_bronze_val", "mhcld_bronze_test", "mhcld_silver_partitioned"]

# Function to check if a table exists and its row count
def check_table(table_name):
    if spark._jsparkSession.catalog().tableExists(table_name):
        count = spark.table(table_name).count()
        print(f"Table '{table_name}' exists and contains {count} rows.")
    else:
        print(f"Table '{table_name}' does not exist.")

# Verify each table
for table in tables:
    check_table(table)

