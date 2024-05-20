--MODULE 2 - LECTURE 

CREATE TABLE module2

(

    col1 FixedString(1),

    col2 UInt32,

    col3 String

)

ENGINE = MergeTree

-- ORDER BY (col1, col2);

PRIMARY KEY (col1, col2);




-- Inserts are performed in bulk. Whether you do it yourself or async insert

-- Each bulk INSERT create a part. A part is stored in its own folder.

-- Every INSERT, these rows has their own folder, they are called parts.

-- async insert - turn it on. DO NOT INSERT ONE ROW AT A TIME!

-- Primary.idx

INSERT INTO module2 (col1, col2, col3) VALUES

    ('B', 1, 'ClickHouse is fast.'),

    ('A', 1, 'Blocks are compressed.'),

    ('B', 2, 'Batch inserts are good.'),

    ('A', 1, 'Small inserts are not great.'),

    ('B', 1, 'How do partitions work?'),

    ('B', 2, 'PKs are stored in primary.idx'),

    ('A', 2, 'How do rows get returned?'),

    ('A', 1, 'Primary keys are sparse.');




show create table module2;

select * from module2;




SHOW CREATE TABLE uk_price_paid;

select * from uk_price_paid LIMIT 20;




SELECT avg(price)

    FROM uk_price_paid

    WHERE postcode1 = 'AL1';

    -- AND postcode2 = '1AJ';




SELECT avg(price)

    FROM uk_price_paid

    WHERE town = 'LONDON';




SELECT count() FROM uk_price_paid;




-- LAB - 2.1 Understanding the primary keys in ClickHouse

-- LAB 2.1 Qus: 1

DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');




-- LAB 2.1 Qus: 2

-- Write a query that returns only the first 10 rows of this file, which will give you an idea of what the dataset looks like.

SELECT * 

    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet')

    LIMIT 10;




-- LAB 2.1 Qus: 3

-- How many rows are in the file?

-- File Format: The second parameter specifies the file format, which in this case is 'parquet'.

-- count(*) => count the total number of rows 

SELECT count(*)

    FROM s3(

        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet',

        'parquet'

    );




-- LAB 2.1 Qus: 4

DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');




CREATE TABLE pypi

(

    TIMESTAMP DateTime,

    COUNTRY_CODE Nullable(String),

    URL Nullable(String),

    PROJECT Nullable(String)

)

ENGINE = MergeTree()

PRIMARY KEY TIMESTAMP;






-- LAB 2.1 Qus: 5

-- Insert all the rows from the Parquet file into the table.

INSERT INTO pypi

SELECT TIMESTAMP, COUNTRY_CODE, URL, PROJECT

FROM s3(

    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet'

);




-- LAB 2.1 Qus: 6

-- Write a query using the count() function that returns the top 100 downloaded projects

-- (i.e. the count() of the PROJECT column).

SELECT

    PROJECT ,

    count(*) AS top_100_downloaded_projects

FROM pypi

GROUP BY PROJECT

ORDER BY top_100_downloaded_projects DESC 

LIMIT 100;




-- LAB 2.1 Qus: 7

-- Looking at the response of the previous query, how many rows in the pypi table were read to compute the result?

-- Elapsed: 0.033s Read: 1,692,671 rows (33.50 MB)




-- LAB 2.1 Qus: 8

-- Re-run the query from Step 6 above that returned the top 100 downloaded projects,

-- but this time filter the results by only downloads that occurred in April of 2023. (

-- Hint: check the toStartOfMonth() or toDate() functions.)

-- https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofmonth

SELECT

    PROJECT ,

    count(*) AS top_100_downloaded_projects

FROM pypi

WHERE toStartOfMonth(TIMESTAMP) = toStartOfMonth(toDateTime('2023-04-01 09:00:00'))

GROUP BY PROJECT

ORDER BY top_100_downloaded_projects DESC 

LIMIT 100;




-- LAB 2.1 Qus: 9

-- How many rows were read by ClickHouse to process the previous query? 

-- Why was it not the entire dataset?

-- Elapsed: 0.031s Read: 565,248 rows (13.47 MB)

-- Filtering with toStartOfMonth: By filtering the data to only include records from April 2023, ClickHouse can skip over large portions of the dataset that do not match this criteria.

-- Primary Key and Indexes: Since the TIMESTAMP field is used as the primary key in the MergeTree table, ClickHouse can efficiently use its primary key index to quickly locate the relevant rows for April 2023. This significantly reduces the number of rows that
 need to be read and processed.




-- LAB 2.1 Qus: 10

-- LAB 2.1 Qus: 11

--==========================================================================================