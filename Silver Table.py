# Databricks notebook source
# MAGIC %md
# MAGIC Convert the categorical variables (GENDER, RACE, ETHNICITY, MARITAL, EMPLOY, INCOME) from numeric values to appropriate string or categorical data types.

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

spark = SparkSession.builder.appName("MHCLD Data - Silver Layer").getOrCreate()


spark.sql("CREATE TABLE IF NOT EXISTS mhcld_bronze_partitioned USING DELTA LOCATION 'dbfs:/user/hive/warehouse/bronze/mhcld_bronze_partitioned'")


bronze_df = spark.table("mhcld_bronze_partitioned")

# Ensure CASEID is stored as an integer data type
silver_df = bronze_df.withColumn("CASEID", col("CASEID").cast(IntegerType()))

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

# Apply mappings to convert numeric values to strings
silver_df = silver_df.withColumn("GENDER", map_column("GENDER", gender_mapping)) \
                     .withColumn("RACE", map_column("RACE", race_mapping)) \
                     .withColumn("ETHNIC", map_column("ETHNIC", ethnicity_mapping)) \
                     .withColumn("MARSTAT", map_column("MARSTAT", marital_mapping)) \
                     .withColumn("EMPLOY", map_column("EMPLOY", employ_mapping))

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

# Validate the data types for all variables
silver_df.printSchema()
display(silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Normalize the numeric variables (MENHLTH, PHYHLTH, POORHLTH)

# COMMAND ----------

# Fill null values in PHYHLTH with the column mean
phyhlth_mean = silver_df.agg(mean("PHYHLTH")).collect()[0][0]
if phyhlth_mean is None:
    phyhlth_mean = 0
silver_df = silver_df.fillna({"PHYHLTH": phyhlth_mean})

# Define a function to check if a column is constant
def is_constant_column(df, column):
    distinct_count = df.select(column).distinct().count()
    return distinct_count == 1

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

# Validate the data types and values for the normalized variables
silver_df.printSchema()
display(silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary of Normalization and Standardization Techniques
# MAGIC
# MAGIC Techniques Used:
# MAGIC Min-Max Scaling:This technique transforms values to a range of [0, 1].
# MAGIC
# MAGIC Rationale:
# MAGIC Min-Max Scaling ensures that all feature values are within the same range, facilitating comparison and improving the performance of algorithms that rely on distance metrics, such as k-nearest neighbors or gradient descent. It handles features with different scales, making the data suitable for models that are sensitive to the magnitude of input values.
# MAGIC
# MAGIC For the dataset, Min-Max Scaling was applied to the `MENHLTH`, `PHYHLTH`, and `POORHLTH` columns. New columns were created to store the normalized values (`MENHLTH_NORM`, `PHYHLTH_NORM`, and `POORHLTH_NORM`), preserving the original data for reference. This approach ensures that the data is scaled appropriately for further analysis while retaining the original values for potential comparison or validation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Partitioning and Sampling

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# Assuming the existing Spark session
spark = SparkSession.builder.appName("MHCLD Data - Stratified Split").getOrCreate()

# Load the silver_df DataFrame (assuming it's already defined)
# silver_df = spark.table("silver_mhcld")  # Uncomment if loading from a table

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
train_df = adjust_schema(train_df, existing_table_schema)
val_df = adjust_schema(val_df, existing_table_schema)
test_df = adjust_schema(test_df, existing_table_schema)

# Function to check if the schema matches the existing table schema
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

if not check_schema_match(train_df.schema, existing_table_schema):
    print_schema_diff(train_df.schema, existing_table_schema)
    raise ValueError("Training set schema does not match the existing table schema.")

if not check_schema_match(val_df.schema, existing_table_schema):
    print_schema_diff(val_df.schema, existing_table_schema)
    raise ValueError("Validation set schema does not match the existing table schema.")

if not check_schema_match(test_df.schema, existing_table_schema):
    print_schema_diff(test_df.schema, existing_table_schema)
    raise ValueError("Testing set schema does not match the existing table schema.")

# Save the training, validation, and testing sets as tables without partitioning
train_df.write.format("delta").mode("overwrite").saveAsTable("mhcld_bronze_train")
val_df.write.format("delta").mode("overwrite").saveAsTable("mhcld_bronze_val")
test_df.write.format("delta").mode("overwrite").saveAsTable("mhcld_bronze_test")

# Drop unnecessary columns from silver_df
silver_df = silver_df.drop("rand", "row_number", "split")

# Adjust the schema of silver_df to match the existing table schema
silver_df = adjust_schema(silver_df, existing_table_schema)

# Partition the silver_df DataFrame by "GENDER" and save it to the silver table
silver_df.write.partitionBy("YEAR").format("delta").mode("overwrite").saveAsTable("mhcld_silver_partitioned")

print("Data saved to the silver table with partitioning.")

