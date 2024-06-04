Below is a sample README file that documents the entire process of transforming raw data into business-ready datasets using the Medallion Architecture.

---

# MHCLD Data Transformation Process

## Overview

This document outlines the process of transforming raw data into business-ready datasets using the Medallion Architecture. The Medallion Architecture organizes data into different layers (bronze, silver, and gold) to streamline data processing and improve query performance.

## Data Sources

The primary data source is the `mhcld_puf_2021` dataset, which contains various demographic, mental health, and service-related fields. The data is initially ingested into the bronze table.

## Bronze Layer

### Purpose
The bronze layer is designed to store raw, unprocessed data. This layer serves as the single source of truth for the ingested data.

### Steps
1. **Data Ingestion**: Raw data is ingested into the bronze table.
2. **Basic Cleaning**: Data types are converted, and negative values are handled appropriately.
3. **Partitioning**: The table is partitioned by `YEAR` to improve query performance.

### SQL Query
```sql
SELECT * 
FROM mhcld_bronze_partitioned
LIMIT 10;
```

## Silver Layer

### Purpose
The silver layer is designed to store cleaned and enriched data. This layer involves more complex transformations to prepare the data for analysis.

### Steps
1. **Data Type Conversion**: Categorical variables (GENDER, RACE, ETHNIC, MARSTAT, EMPLOY) are converted from numeric values to appropriate string categories.
2. **New Columns**: New columns (`MENHLTH`, `PHYHLTH`, `POORHLTH`) are calculated based on specific conditions.
3. **Normalization**: Numeric columns (`MENHLTH`, `PHYHLTH`, `POORHLTH`) are normalized using min-max scaling.
4. **Stratified Sampling**: The data is split into training, validation, and testing sets while maintaining the proportions of demographic variables.
5. **Partitioning**: The table is partitioned by `YEAR` to improve query performance.

### SQL Query
```sql
SELECT * 
FROM mhcld_silver_partitioned
LIMIT 10;
```

## Gold Layer

### Purpose
The gold layer is designed to store the final, curated data. This layer includes business-ready datasets with additional calculated fields and optimized for query performance.

### Steps
1. **Combining Datasets**: Training, validation, and testing sets from the silver table are combined into a single DataFrame.
2. **Additional Calculations**: New business-ready fields such as `AGE_GROUP` are added.
3. **Schema Adjustment**: The schema of the combined dataset is adjusted to match the existing table schema.
4. **Partitioning**: The table is partitioned by `YEAR` and `AGE_GROUP` to optimize query performance.

### SQL Query
```sql
SELECT * 
FROM mhcld_gold_partitioned
LIMIT 10;
```

## Rationale for Medallion Architecture

The Medallion Architecture provides a structured approach to data processing by organizing data into different layers:

1. **Bronze Layer**: Acts as the landing zone for raw data, ensuring data integrity and providing a single source of truth.
2. **Silver Layer**: Facilitates data cleaning, enrichment, and normalization. This layer prepares the data for more complex analysis.
3. **Gold Layer**: Contains the final, curated datasets optimized for business use. This layer includes additional calculations and partitioning for improved query performance.

## Monitoring and Schema Checks

### Monitoring
To track the success and failure of the data processing pipeline, logging is implemented at each step. Logs capture key events and any errors that occur during the process.

### Schema Checks
Schema checks are performed to ensure that the final data meets the expected schema. Functions are implemented to adjust and validate the schema of the DataFrames against the existing table schema. Any schema mismatches are logged and raised as errors.