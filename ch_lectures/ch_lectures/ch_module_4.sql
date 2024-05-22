-- MODULE 4 LECTURE - Inserting data into ClickHouse
-- Input format
-- Output format
-- There are countless ways to insert data into ClickHouse
-- ETL , clickhouse connector as sink

-- Two questions to ask : 1. Where is your data now? (table function to read it) 2. What format is your data in? (Input format to read it)
-- https://clickhouse.com/docs/en/sql-reference/table-functions
-- Cloud storage
-- data is sent to you. For these, needs table engine 

-- Option 1
DESC s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet');


-- Option2 
-- Enabled (schema_inference_make_columns_nullable = 1): Every field might be nullable.
-- Disabled (schema_inference_make_columns_nullable = 0) with input_format_null_as_default = 0: Fields are nullable only if explicitly shown as null.
-- Disabled (schema_inference_make_columns_nullable = 0) with input_format_null_as_default = 1: Fields are not nullable, even if shown as null, they get a default value.

DESC s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
SETTINGS schema_inference_make_columns_nullable = 0;

SELECT * 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
LIMIT 100;

-- formatReadableQuantity(x)
-- Given a number, this function returns a rounded number with suffix (thousand, million, billion, etc.) as string.
-- 41.91 million
SELECT formatReadableQuantity(count())
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet');

CREATE TABLE amazon_reviews (
    review_date Date ,
    marketplace	LowCardinality(String) ,
    customer_id	UInt64 ,
    review_id	String ,
    product_id	String ,
    product_parent	UInt64 ,
    product_title	String ,
    product_category	LowCardinality(String) ,
    star_rating	UInt8 ,
    helpful_votes	UInt32 ,
    total_votes	UInt32 ,
    vine	Bool ,
    verified_purchase	Bool ,
    review_headline	String ,
    review_body	String
)
ENGINE = MergeTree
PRIMARY KEY (review_date, product_category);

INSERT INTO amazon_reviews
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet');

SELECT formatReadableQuantity(count()) from amazon_reviews;

-- The name of each disk (disk_name).
-- The total compressed size of data on each disk, formatted to a readable size (compressed).
-- The total uncompressed size of data on each disk, formatted to a readable size (uncompressed).
-- The compression rate of data on each disk (compr_rate).
-- The total number of rows stored on each disk (rows).
-- The number of parts stored on each disk (part_count).
-- Clickhouse compressed at 2.35 times more 

SELECT
    disk_name,
    formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed,
    round(usize / size, 2) AS compr_rate,
    sum(rows) AS rows,
    count() AS part_count
FROM system.parts
WHERE (active=1) AND (table='amazon_reviews')
GROUP BY disk_name
ORDER BY size DESC;


SELECT * 
FROM amazon_reviews
LIMIT 1000;

-- Product with the most view 
SELECT product_id
FROM amazon_reviews
WHERE revie

SELECT 
    any(product_title),
    count()
FROM amazon_reviews
GROUP BY product_id
ORDER BY count() DESC
LIMIT 20;

SELECT 
    min(review_date) AS Least_reviews_date,
    max(review_date) AS Most_reviews_date
FROM amazon_reviews;

-- INCORRECT ONE
SELECT 
    (min(review_date), countIf(review_date = min(review_date))) AS Least_reviews_date_and_count,
    (max(review_date), countIf(review_date = max(review_date))) AS Most_reviews_date_and_count
FROM amazon_reviews;

-- Putting date and review number together. Tuple is more appropriate than array since they are different data types
-- A common table expression, or CTE, (in SQL) is a temporary named result set, derived from a simple query and 
-- defined within the execution scope of a SELECT, INSERT, UPDATE, or DELETE statement.
-- CTEs can be thought of as alternatives to derived tables (subquery), views, and inline user-defined functions.
-- Common Table Expressions (CTEs):
-- min_review_date: Finds the minimum review date.
-- max_review_date: Finds the maximum review date.
-- min_date_count: Counts the number of reviews on the minimum review date.
-- max_date_count: Counts the number of reviews on the maximum review date.
WITH 
    min_review_date AS (
        SELECT 
            min(review_date) AS min_date
        FROM amazon_reviews
    ),
    max_review_date AS (
        SELECT 
            max(review_date) AS max_date
        FROM amazon_reviews
    ),
    min_date_count AS (
        SELECT 
            count(*) AS min_date_count
        FROM amazon_reviews
        WHERE review_date = (SELECT min_date FROM min_review_date)
    ),
    max_date_count AS (
        SELECT 
            count(*) AS max_date_count
        FROM amazon_reviews
        WHERE review_date = (SELECT max_date FROM max_review_date)
    )
SELECT 
    (min_date, min_date_count) AS Least_reviews_date_and_count,
    (max_date, max_date_count) AS Most_reviews_date_and_count
FROM 
    min_review_date, max_review_date, min_date_count, max_date_count;

-- =================== TERMINOLOGIES
-- ** Common table expression
-- subqueries
-- sink 
-- table engine stores the connection details, types of files

-- TABLE FUNCTIONS
-- https://clickhouse.com/docs/en/getting-started/example-datasets/amazon-reviews
-- inference,


