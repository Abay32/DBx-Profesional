# Databricks notebook source
dbutils.fs.ls("s3://dalhussein-courses/DE-Pro/datasets/bookstore/v1/")



# COMMAND ----------

dbutils.fs.cp("s3://dalhussein-courses/DE-Pro/datasets/bookstore/v1/kafka-streaming", "/Volumes/dev/pro_landing_zone/kafka_sources/books_kafka_row/", recurse=True)

# COMMAND ----------

df_raw = spark.read.json("/Volumes/dev/pro_landing_zone/kafka_sources/books_kafka_row/")

df_raw.show()

# COMMAND ----------

# DBTITLE 1,Cell 4
# Incremental data processing from Bronze to Silver
from pyspark.sql import functions as F
def process_bronze():
    df = (
      spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .schema (schema="key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
          .option("pathGlobFilter", "*.json")
          .load("/Volumes/dev/pro_landing_zone/kafka_sources/books_kafka_row/")
          .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
          .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
        .writeStream
          .option("checkpointLocation","/Volumes/dev/pro_landing_zone/checkpoints/bronze/")
          .option("mergeSchema", True)
          .partitionBy("topic", "year_month")
          .trigger(availableNow=True)
          .table("dev.multiplex_bronze.kafka_bronze")
    )
    df.awaitTermination()

process_bronze()
  

# COMMAND ----------


# drop table dev.multiplex_bronze.kafka_bronze
dbutils.fs.rm("/Volumes/dev/pro_landing_zone/checkpoints/bronze", True)

# COMMAND ----------

df = spark.table("dev.multiplex_bronze.kafka_bronze")
display(df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CAST(key AS STRING), 
# MAGIC   CAST(value AS STRING) 
# MAGIC FROM dev.multiplex_bronze.kafka_bronze
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC     SELECT from_json(
# MAGIC         CAST(value AS STRING),
# MAGIC         "order_id STRING, 
# MAGIC         order_timestamp Timestamp, 
# MAGIC         customer_id STRING, 
# MAGIC         quantity BIGINT, 
# MAGIC         total BIGINT, 
# MAGIC         books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
# MAGIC     ) v
# MAGIC     FROM dev.multiplex_bronze.kafka_bronze
# MAGIC     WHERE topic = 'orders'
# MAGIC )

# COMMAND ----------

# convert our static table to streaming view 
(
    spark.readStream
      .table("dev.multiplex_bronze.kafka_bronze")
      .createOrReplaceTempView("kafka_bronze_view")
)

# COMMAND ----------

df = spark.sql("SELECT * FROM kafka_bronze_view")
display(df, checkpointLocation="/Volumes/dev/pro_landing_zone/checkpoints/display_kafka_bronze/")

# COMMAND ----------

    
query_view = f"""
  SELECT v.*
  FROM(
      SELECT from_json(
      CAST(value AS STRING),
      "order_id STRING, 
      order_timestamp Timestamp, 
      customer_id STRING, 
      quantity BIGINT, 
      total BIGINT, 
      books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
  ) v
  FROM kafka_bronze_view
  WHERE topic = 'orders')
"""

df_view = spark.sql(query_view)
display(df_view, checkpointLocation="/Volumes/dev/pro_landing_zone/checkpoints/kafka_bronze_view/")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a  Temporary view orders_silver_temp from the streaming temporary view 
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_temp AS (
# MAGIC     SELECT v.*
# MAGIC     FROM (
# MAGIC         SELECT from_json(
# MAGIC             CAST(value AS STRING),
# MAGIC             "order_id STRING, 
# MAGIC             order_timestamp Timestamp, 
# MAGIC             customer_id STRING, 
# MAGIC             quantity BIGINT, 
# MAGIC             total BIGINT, 
# MAGIC             books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
# MAGIC         ) v
# MAGIC         FROM dev.multiplex_bronze.kafka_bronze
# MAGIC         WHERE topic = 'orders'
# MAGIC     ) 
# MAGIC )
# MAGIC

# COMMAND ----------

df = spark.table("orders_silver_temp")
df.show()


# COMMAND ----------

# Process orders from bronze to silver
from pyspark.sql import functions as F

def process_orders():
    df = (
        spark.readStream
            .table("dev.multiplex_bronze.kafka_bronze")
            .filter(F.col("topic") == "orders")
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
                ).alias("v")
            )
            .select("v.*")
            .filter(F.col("quantity") > 0)
            .writeStream
            .option("checkpointLocation", "/Volumes/dev/pro_landing_zone/checkpoints/silver_orders/")
            .trigger(availableNow=True)
            .table("dev.silver.orders_silver")
    )
    df.awaitTermination()

process_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM 
# MAGIC   dev.silver.orders_silver
# MAGIC WHERE quantity <= 0
