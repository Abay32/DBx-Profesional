CREATE OR REFRESH STREAMING TABLE bookstore_ldp_catalog.silver.books_silver;

CREATE FLOW books_flow
AS AUTO CDC INTO bookstore_ldp_catalog.silver.books_silver
FROM STREAM(books_raw)
KEYS (book_id)
SEQUENCE BY updated
COLUMNS * EXCEPT (updated)
STORED AS SCD TYPE 2;

CREATE OR REFRESH MATERIALIZED VIEW bookstore_ldp_catalog.silver.current_books 
AS 
SELECT 
  book_id,
  title,
  author,
  price
FROM
  bookstore_ldp_catalog.silver.books_silver
WHERE __END_AT IS NULL;


