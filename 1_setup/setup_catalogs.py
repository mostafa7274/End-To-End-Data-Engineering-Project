# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create ONE catalog
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC
# MAGIC -- Switch to it
# MAGIC USE CATALOG fmcg;
# MAGIC
# MAGIC -- Create schemas (NOT catalogs)
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fmcg.gold.fact_orders

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

