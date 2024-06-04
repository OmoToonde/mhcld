# Databricks notebook source
# MAGIC %md
# MAGIC QUERY FROM BRONZE TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mhcld_bronze
# MAGIC LIMIT 100;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC QUERY FROM SILVER TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mhcld_silver_partitioned
# MAGIC LIMIT 100;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC QUERY FROM GOLD TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mhcld_gold_partitioned
# MAGIC LIMIT 100;
# MAGIC
