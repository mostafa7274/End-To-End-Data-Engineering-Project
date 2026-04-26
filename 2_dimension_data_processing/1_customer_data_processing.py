# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/mostafatarek7274@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema , silver_schema , gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog" , "fmcg" , "Catalog")
dbutils.widgets.text("data_source" , "customers" , "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")


base_path = f's3://sportsbar---db/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = spark.read.format("csv").load(base_path)
display(df.limit(10))

# COMMAND ----------

df= (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp" , f.current_timestamp())
    .select("*" , "_metadata.file_name" , "_metadata.file_size")
)
display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed" , "true") \
            .mode("overwrite") \
                .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source};" )

df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping Duplicates

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().where("count > 1")

display(df_duplicates)

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())


# COMMAND ----------

# MAGIC %md
# MAGIC Trimming Spaces in customer names
# MAGIC

# COMMAND ----------

# check those values
display(
    df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name")))
)

# COMMAND ----------

## remove those trim values

df_silver = df_silver.withColumn(
    "customer_name",
    f.trim(f.col("customer_name"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking Data Quality 

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# # typo dictionary
# city_typos = {
#     'Bengaluru': ['Bengaluruu', 'Bengaluruu', 'Bengalore'],
#     'Hyderabad': ['Hyderabadd', 'Hyderbad'],
#     'New Delhi': ['NewDelhi', 'NewDheli', 'NewDelhee']
# }

# typos → correct names
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}


allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        f.when(f.col("city").isNull(), None)
         .when(f.col("city").isin(allowed), f.col("city"))
         .otherwise(None)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Fixing Title Case Issue

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
  "customer_name" , 
  f.when(f.col("customer_name").isNull(),None)
  .otherwise(f.initcap("customer_name"))
)
#initcap means the first letter is capital and the other is small

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Handling Missing Cities

# COMMAND ----------

df_silver.filter(f.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(f.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)
#items() converts the dictionary into key-value pairs

# COMMAND ----------

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        f.coalesce("city", "fixed_city")   # Replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC converting customer_id column into string

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id" , f.col("customer_id").cast("string"))

print(df_silver.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing Customer Attributes to Match Parent Company Data Model
# MAGIC

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        f.concat_ws("-", "customer_name", f.coalesce(f.col("city"), f.lit("Unknown")))
    )
    
    # Static attributes aligned with parent data model
    .withColumn("market", f.lit("India"))
    .withColumn("platform", f.lit("Sports Bar"))
    .withColumn("channel", f.lit("Acquisition"))
)


# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")


# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")
# selecting only the needed columns for the next layer (Gold)

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")
 # sb stands for sports bar

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merging data source with parent data
# MAGIC

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    f.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
# This is called absert operation

# COMMAND ----------



# COMMAND ----------

