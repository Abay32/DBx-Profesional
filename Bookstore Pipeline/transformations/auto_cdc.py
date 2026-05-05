from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.temporary_view(
    name="customers_bronze_cdc", 
    comment="This is a temporary view for customers data: used as a target tabel for auto_cdc process"
)
def customers_bronze_cdc(): 
    cust_schema = """
        customer_id STRING,
        email STRING,
        first_name STRING,
        last_name STRING,
        gender STRING,
        street STRING,
        city STRING,
        country_code STRING,
        row_status STRING,
        row_time TIMESTAMP
    """
    country_lookup_df = spark.read.table("country_lookup")


    return (
        spark.readStream
            .table("bookstore_ldp_catalog.bronze.multiplex_bronze")
            .filter("topic = 'customers'")
            .select(F.from_json(F.col("value").cast("string"), schema = cust_schema).alias("cust"))
            .select("cust.*")
            .filter(F.col("row_status").isin(["insert", "update"]))
            .join(F.broadcast(country_lookup_df), F.col("country_code")==F.col("code"), "inner")
    )

dp.create_streaming_table("bookstore_ldp_catalog.silver.customers_silver")

dp.create_auto_cdc_flow(
    target="bookstore_ldp_catalog.silver.customers_silver",
    source= "customers_bronze_cdc",  
    keys=["customer_id"],
    sequence_by=F.col("row_time"),
    except_column_list=["row_status", "row_time"]
    #apply_as_deletes = F.expr("row_status = 'delete'")
)
    
@dp.materialized_view(
    name="bookstore_ldp_catalog.gold.countries_stats",
    comment="This is a materialized view for countries stats for business consumption"
)
def countries_stats():
    orders_df = spark.read.table("bookstore_ldp_catalog.silver.orders_silver")
    customers_df = spark.read.table("bookstore_ldp_catalog.silver.customers_silver")
    return (
        orders_df
            .join(customers_df, ["customer_id"], "inner")
            .withColumn("order_date", F.date_trunc("DAY", F.col("order_timestamp")))
            .groupBy("country", "order_date")
            .agg(
                F.count("order_id").alias("orders_count"),
                F.sum("quantity",).alias("books_count")
            )
    )

@dp.table(
    name="bookstore_ldp_catalog.gold.authors_stats",
    comment="Aggregated statistics of book sales per author in 5-minutes windows"
)
def authors_stats():
    orders_df = (spark.readStream.table("bookstore_ldp_catalog.silver.books_sales_silver")
            .withWatermark("order_timestamp", "5 minutes")
            .groupBy(
                F.window("order_timestamp", "5 minutes"),
                "author"
            ).agg(
                F.avg("quantity").alias("avg_quantity"),
                F.count("order_id").alias("orders_count")
            )
    )

    return orders_df



