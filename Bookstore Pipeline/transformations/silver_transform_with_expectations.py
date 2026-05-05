from pyspark import pipelines as dp
from pyspark.sql import functions as F

def process_orders():
    orders_schema = """
        order_id STRING,
        order_timestamp TIMESTAMP,
        customer_id STRING,
        quantity BIGINT,
        total BIGINT,
        books ARRAY<STRUCT<
            book_id STRING,
            quantity BIGINT,
            subtotal BIGINT
        >>
    """
    orders_df = (
        spark.readStream
            .table("bookstore_ldp_catalog.bronze.multiplex_bronze")
            .filter("topic = 'orders'")
            .select(F.from_json(
                F.col("value").cast("string"), 
                orders_schema).alias("orders")
            ).select("orders.*")
    )

    return orders_df


@dp.table(
    name="bookstore_ldp_catalog.silver.orders_silver",
    comment="Orders in transfromation ..."
)
@dp.expect_or_drop("Valid quantity", "quantity > 0") 
def orders_silver():
    return process_orders()


@dp.table(
    name="bookstore_ldp_catalog.silver.orders_quarantine",
    comment="Orders where its quantity is below or zero is stored in quarantine for next analysis"
)
@dp.expect("Invalid quantity", "quantity <= 0") 
def orders_quarantine():
    return process_orders()


# ----------------- Boooks Section-------------------------------------------------------

rules = {
    "recent_updates": "updated >= '2020-01-01'",
    "valid_price": "price > 0",
    "valid_id": "book_id IS NOT NULL"
}

quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))
@dp.temporary_view(name="books_raw") 
@dp.expect_all(rules)
def books_raw():
    books_schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
    return (
        spark.readStream
            .table("bookstore_ldp_catalog.bronze.multiplex_bronze")
            .filter("topic = 'books'")
            .select(F.from_json(F.col("value").cast("string"), books_schema).alias("books"))
            .select("books.*")
            .withColumn("is_quarantined", F.expr(quarantine_rules))
    ) 





# ------------ Books Sales -------------------------------- 
@dp.table(
    name="bookstore_ldp_catalog.silver.books_sales_silver",
    comment="Books sales transformation"
)
def books_sales_silver():
    orders_df = (
        spark.readStream
            .table("bookstore_ldp_catalog.silver.orders_silver")
            .withWatermark("order_timestamp", "1 day")
            .withColumn("book", F.explode("books"))
    )
    books_df = (
        spark.table("bookstore_ldp_catalog.silver.current_books")
            .select("book_id", "title", "author", "price")
    )
    result_df = (
        orders_df.join(
            books_df,
            orders_df.book.book_id == books_df.book_id,
            "inner"
        )
    )
    return result_df



