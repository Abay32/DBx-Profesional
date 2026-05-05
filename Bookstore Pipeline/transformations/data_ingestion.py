from pyspark import pipelines as dp
from pyspark.sql import functions as F

dataset_path = "/Volumes/bookstore_ldp_catalog/landing/kafka_source" # This is the base path for volumes

catalog_schema = "bookstore_ldp_catalog.bronze"

@dp.table(
    name=f"{catalog_schema}.multiplex_bronze", 
    comment="Multiplex bronze: multiplexes all bronze tables",
    partition_cols=["topic", "year_month"],
    table_properties = {
        "delta.appendOnly": "true",
        "pipelines.reset.allowed": "false"
    }
)
def multiplex_bronze():
    schema = "key BINARY, value BINARY, topic STRING, partition STRING, offset BIGINT, timestamp LONG"
    multiplex_bronze = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(schema)
            .load(f"{dataset_path}/kafka-raw/")
            .withColumn("timestamp", (F.col("timestamp") / 1000).cast("timestamp"))
            .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
    )
    return multiplex_bronze


# Create a temporary view for the country_lookup dataset
@dp.temporary_view(name="country_lookup")
def country_lookup(): 
    return spark.read.json(f"{dataset_path}/country_lookup/*")
    