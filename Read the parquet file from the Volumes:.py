# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("MHCLD Data").getOrCreate()

# Define the correct file path
file_path = "dbfs:/user/hive/warehouse/mhcld_puf_2021"

# Load the Delta table into a DataFrame
mhcld_df = spark.read.format("delta").option("header", "true").option("inferSchema", "false").load(file_path)

# Show the first few rows of the DataFrame
mhcld_df.show()

# COMMAND ----------

mhcld_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Print the schema of the DataFrame

# COMMAND ----------


mhcld_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Show summary statistics for numerical columns

# COMMAND ----------

mhcld_df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Show the data types of all columns

# COMMAND ----------

mhcld_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Define the expected schema as a list of tuples (field name, field type)

# COMMAND ----------

expected_schema = [
    ("YEAR", "int"),
    ("AGE", "int"),
    ("EDUC", "int"),
    ("ETHNIC", "int"),
    ("RACE", "int"),
    ("GENDER", "int"),
    ("SPHSERVICE", "int"),
    ("CMPSERVICE", "int"),
    ("OPISERVICE", "int"),
    ("RTCSERVICE", "int"),
    ("IJSSERVICE", "int"),
    ("MH1", "int"),
    ("MH2", "int"),
    ("MH3", "int"),
    ("SUB", "int"),
    ("MARSTAT", "int"),
    ("SMISED", "int"),
    ("SAP", "int"),
    ("EMPLOY", "int"),
    ("DETNLF", "int"),
    ("VETERAN", "int"),
    ("LIVARAG", "int"),
    ("NUMMHS", "int"),
    ("TRAUSTREFLG", "int"),
    ("ANXIETYFLG", "int"),
    ("ADHDFLG", "int"),
    ("CONDUCTFLG", "int"),
    ("DELIRDEMFLG", "int"),
    ("BIPOLARFLG", "int"),
    ("DEPRESSFLG", "int"),
    ("ODDFLG", "int"),
    ("PDDFLG", "int"),
    ("PERSONFLG", "int"),
    ("SCHIZOFLG", "int"),
    ("ALCSUBFLG", "int"),
    ("OTHERDISFLG", "int"),
    ("STATEFIP", "int"),
    ("DIVISION", "int"),
    ("REGION", "int"),
    ("CASEID", "int")
]

# COMMAND ----------

# MAGIC %md
# MAGIC Validate the schema and Compare schemas 

# COMMAND ----------


actual_schema = mhcld_df.dtypes
mismatched_columns = []
for expected_col, expected_type in expected_schema:
    for actual_col, actual_type in actual_schema:
        if expected_col == actual_col and expected_type != actual_type:
            mismatched_columns.append((expected_col, expected_type, actual_type))
            break
    else:
        if expected_col not in [col for col, _ in actual_schema]:
            mismatched_columns.append((expected_col, expected_type, "Column missing"))

schema_matches = len(mismatched_columns) == 0
print(f"Schema matches: {schema_matches}")

# COMMAND ----------

# MAGIC %md
# MAGIC Check for null values in non-nullable columns

# COMMAND ----------

non_nullable_columns = ["CASEID", "YEAR", "AGE", "EDUC", "ETHNIC", "RACE", "GENDER"]
null_values_check = mhcld_df.filter(
    (mhcld_df.CASEID.isNull()) |
    (mhcld_df.YEAR.isNull()) |
    (mhcld_df.AGE.isNull()) |
    (mhcld_df.EDUC.isNull()) |
    (mhcld_df.ETHNIC.isNull()) |
    (mhcld_df.RACE.isNull()) |
    (mhcld_df.GENDER.isNull())
).count()

print(f"Number of rows with null values in non-nullable columns: {null_values_check}")


# COMMAND ----------

# MAGIC %md
# MAGIC Check for specific constraints (e.g., AGE should be between 0 and 120)
# MAGIC

# COMMAND ----------

invalid_age_count = mhcld_df.filter((mhcld_df.AGE < 0) | (mhcld_df.AGE > 120)).count()
print(f"Number of rows with invalid AGE: {invalid_age_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Check for specific constraints (e.g., AGE should be between 0 and 120)

# COMMAND ----------

invalid_age_df = mhcld_df.filter((mhcld_df.AGE < 0) | (mhcld_df.AGE > 120))
invalid_age_count = invalid_age_df.count()
print(f"Number of rows with invalid AGE: {invalid_age_count}")

# Show a few rows with invalid AGE values
invalid_age_df.display(10)

# COMMAND ----------

total_row_count = mhcld_df.count()
print (f"total_row_count: {total_row_count}")
