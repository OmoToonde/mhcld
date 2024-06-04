# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# COMMAND ----------

# MAGIC %md
# MAGIC Create a bronze table to store the raw data from the CSV file.

# COMMAND ----------

# Define the file path for the raw data
file_path = "dbfs:/user/hive/warehouse/mhcld_puf_2021"

# Load the raw data into a DataFrame
raw_df = spark.read.format("delta").load(file_path)

# Display the first few rows of the DataFrame to verify
raw_df.display()

# Define the path for the Bronze table
bronze_table_path = "dbfs:/user/hive/warehouse/bronze/mhcld_bronze"

# Write the raw data to the Bronze table as a Delta table
raw_df.write.format("delta").mode("overwrite").save(bronze_table_path)

# Register the Bronze table in the Hive metastore for SQL queries
spark.sql("CREATE TABLE IF NOT EXISTS mhcld_bronze USING DELTA LOCATION 'dbfs:/user/hive/warehouse/bronze/mhcld_bronze'")

print("Bronze table created and data stored.")


# COMMAND ----------

bronze_table_path = "dbfs:/user/hive/warehouse/bronze/mhcld_bronze"

# Load the Bronze table into a DataFrame
bronze_df = spark.read.format("delta").load(bronze_table_path)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# Replace -9 with appropriate values for specified columns
columns_to_replace_zero = ["MH1", "MH2", "MH3", "SUB", "SMISED", "SAP", "TRAUSTREFLG", 
                           "ANXIETYFLG", "ADHDFLG", "CONDUCTFLG", "DELIRDEMFLG", 
                           "BIPOLARFLG", "DEPRESSFLG", "ODDFLG", "PDDFLG", 
                           "PERSONFLG", "SCHIZOFLG", "ALCSUBFLG", "OTHERDISFLG"]

columns_to_replace_minus_one = ["AGE", "EDUC", "ETHNIC", "RACE", "GENDER", "MARSTAT", 
                                "EMPLOY", "DETNLF", "VETERAN", "LIVARAG"]

for column in columns_to_replace_zero:
    bronze_df = bronze_df.withColumn(column, when(col(column) == -9, 0).otherwise(col(column)))

for column in columns_to_replace_minus_one:
    bronze_df = bronze_df.withColumn(column, when(col(column) == -9, -1).otherwise(col(column)))

# Optionally drop rows with too many nulls (e.g., more than 50% null values)
threshold = int(len(bronze_df.columns) * 0.5)
cleaned_df = bronze_df.dropna(thresh=threshold)

# Filter out rows where AGE is less than 1
filtered_df = cleaned_df.filter(cleaned_df.AGE >= 1)

# Define the path for the partitioned Bronze table
partitioned_bronze_table_path = "dbfs:/user/hive/warehouse/bronze/mhcld_bronze_partitioned"

# Write the filtered and cleaned data to a new Delta table partitioned by the YEAR column
filtered_df.write.format("delta").mode("overwrite").partitionBy("YEAR").save(partitioned_bronze_table_path)

# Register the partitioned Bronze table in the Hive metastore for SQL queries
spark.sql("CREATE TABLE IF NOT EXISTS mhcld_bronze_partitioned USING DELTA LOCATION 'dbfs:/user/hive/warehouse/bronze/mhcld_bronze_partitioned'")

print("Partitioned Bronze table created and data stored.")


# COMMAND ----------

filtered_df.display()
