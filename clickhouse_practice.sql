SELECT * from system.functions where name = 'sum'

-- Create a new database
CREATE DATABASE IF NOT EXISTS helloworld

-- Create table
-- Primary key in ClickHouse is not unique for each row in a table.
CREATE TABLE helloworld.my_first_table
(
    user_id UInt32
    message String 
    timestamp DateTime
    metric Float32 
)
ENGINE = MergeTree()
PRIMARY KEY = (user_id, timestamp)
--=====================================
-- Create a view for terraced properties in UK
-- This is the normal view, not a materialized view, it is not a table, but is saved as a query
CREATE VIEW uk_terraced_property
AS 
    select *
    from uk_price_paid
    where type = 'terraced';

-- This is what happended behind the scene
select count() from (
    select *
    from uk_price_paid
    where type = 'terraced'
)

--================================
-- https://clickhouse.com/codebrowser/ClickHouse/src/Functions/formatReadableQuantity.cpp.html
-- formatReadableQuantity(x) - Given a number, this function returns a rounded number with suffix (thousand, million, billion, etc.) as string.
-- Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows if each row consists of a large number of columns.
-- NULL is output as ᴺᵁᴸᴸ.
select
    formatReadableQuantity(countIf( type = 'terraced')) as terraced,
    formatReadableQuantity(countIf( type = 'semi-detached')) as semi_detached,
    formatReadableQuantity(countIf( type = 'detached')) as detached,
    formatReadableQuantity(countIf( type = 'flat')) as flat,
    formatReadableQuantity(countIf( type = 'other')) as other
FROM uk_price_paid
FORMAT Vertical

--=================================
-- Add if to an aggregate function, street is not an empty string
select 
    topKIf(10)(street, street != '')
    from uk_price_paid;
--=================================
select uniq(street) from uk_price_paid;

-- Use more memory because it is exact
select uniqExact(street) from uk_price_paid;

-- Top 10 most frequently occuring street names
select
    topK(10)(street)
    from uk_price_paid;
--=================================
-- What is 90 percentile of the price of UK home sold
select 
    quantile(0.9)(price)
from uk_price_paid;

select
    quantiles(0.1,0.5,0.9)(price)
from uk_price_paid;
--=================================
show tables;
--=================================
select
    town,
    count() as number_of_sales
from uk_price_paid
group by town 
order by number_of_sales desc
limit 10
format JSONEachRow
--=================================
with
    'LONDON' as my_town
select
    avg(price)
from uk_price_paid
where town = my_town;

with most_expensive as (
    select * from uk_price_paid
    order by price desc
    limit 10
)
select
    avg(price)
from most_expensive;
--=================================
select count() from system.functions;
--=================================
with 
    now() as time
select
    toTimeZone(time, 'Asia/Tokyo'),
    toDate(time),
    toYYYYMM(time),
    time-1,
    toStartOfMonth(time),
    addWeeks(time,2),
    formatDateTime(time, '%Y,%m,%d');
--==========================================
select
    count()
from uk_price_paid
where 
    position(street, 'KING') > 0;
--=================================
select 
    count()
from uk_price_paid
where
    multiFuzzyMatchAny(street, 1, ['KING']);


--========================================
select distinct
    street,
    multiSearchAllPositionsCaseInsensitive(
        street,
        ['abbey', 'road']
    ) as positions
from uk_price_paid
where not has (positions,0);
--=======================================
select
    town,
    max(price),
    -- instead of using subquery, you can use argMax function
    argMax(street, price)
from uk_price_paid
group by town;
--=====================================
select count() from system.functions;
create function mergePostCode as (p1, p2) -> concat(p1,p2);

select * from uk_price_paid where p1 != "" limit 10;
select mergePostCode(postcode1, postcode2) from uk_price_paid
    where postcode1 != '' limit 10;

--========================================================
CREATE TABLE japan_cities
(
    number Int8,
    city_name String,
    famous_food String,
    best_season_to_visit String
)
PRIMARY KEY(city_name)

SHOW databases;
SHOW TABLES from default;
DESCRIBE default.japan_cities;

SHOW CREATE japan_cities;

INSERT INTO default.japan_cities (number, city_name, famous_food, best_season_to_visit)
    VALUES 
        (4, 'Nigata', 'Soba', 'Spring'),
        (2, 'Kanazawa', 'Seafood', 'Winter'),
        (3, 'Osaka', 'Takoyaki', 'Autumn');

SELECT * FROM
    default.japan_cities;

USE japan_cities;

INSERT INTO japan_cities(*) VALUES 
INSERT INTO japan_cities(*) VALUES 
--===========================================================================
-- CREATE A TABLE
CREATE TABLE crypto_prices_hello
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree()
PRIMARY KEY (crypto_name, trade_date)
DROP TABLE crypto_prices_hello;

-- LAB 3 : Insert crypto_prices table with data stored in a CSV file in S3
INSERT INTO crypto_prices 
    SELECT *
    FROM s3(
        'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv'
    )


DESCRIBE url('http  ://prod2.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv')

SELECT * from url('http://prod2.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv',
    'CSV')
LIMIT 200;

-- https://clickhouse.com/docs/en/getting-started/example-datasets/uk-price-paid

-- Create UK House pirce table 
CREATE TABLE uk_price_paid
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2);

-- Insert Data into the newly created table
INSERT INTO uk_price_paid
WITH
   splitByChar(' ', postcode) AS p
SELECT
    toUInt32(price_string) AS price,
    parseDateTimeBestEffortUS(time) AS date,
    p[1] AS postcode1,
    p[2] AS postcode2,
    transform(a, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other']) AS type,
    b = 'Y' AS is_new,
    transform(c, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown']) AS duration,
    addr1,
    addr2,
    street,
    locality,
    town,
    district,
    county
FROM url(
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv',
    'CSV',
    'uuid_string String,
    price_string String,
    time String,
    postcode String,
    a String,
    b String,
    c String,
    addr1 String,
    addr2 String,
    street String,
    locality String,
    town String,
    district String,
    county String,
    d String,
    e String'
) SETTINGS max_http_get_redirects=10;

SELECT count() from uk_price_paid;

SELECT formatReadableQuantity(count()) from uk_price_paid;

SELECT formatReadableQuantity(total_bytes)
    FROM system.tables
    WHERE name = 'uk_price_paid';

-- Query 1. Average Price Per Year
SELECT
   toYear(date) AS year,
   round(avg(price)) AS price,
   bar(price, 0, 1000000, 80
)
FROM uk_price_paid
GROUP BY year
ORDER BY year

-- Query 2. Average Price per Year in London
SELECT
   toYear(date) AS year,
   round(avg(price)) AS price,
   bar(price, 0, 2000000, 100
)
FROM uk_price_paid
WHERE town = 'LONDON'
GROUP BY year
ORDER BY year

-- Query 3. The Most Expensive Neighborhoods
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM uk_price_paid
WHERE date >= '2020-01-01'
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 100
