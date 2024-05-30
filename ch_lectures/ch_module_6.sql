---=========== MODULE -6 Materialized Views =========== 
--======================================
-- Normal
-- regular view - VIEW
CREATE DATABASE su_clickhouse_practice_db;
-- CREATE uk_price_paid
-- https://clickhouse.com/docs/en/getting-started/example-datasets/uk-price-paid
-- MPP , Sharding
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

-- Drop table
drop table uk_price_paid;

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

-- CHECK table
SHOW CREATE TABLE uk_price_paid;

--- SELECT COUNT
select formatReadableQuantity(count()) from uk_price_paid;

---=========== MODULE -6 Materialized Views =========== 
--======================================
-- Normal
-- regular view - VIEW
CREATE DATABASE su_clickhouse_practice_db;
-- CREATE uk_price_paid
-- https://clickhouse.com/docs/en/getting-started/example-datasets/uk-price-paid
-- MPP , Sharding
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

-- Drop table
drop table uk_price_paid;

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

-- CHECK table
SHOW CREATE TABLE uk_price_paid;

--- SELECT COUNT
select formatReadableQuantity(count()) from uk_price_paid;

-- Normal views are just stored SELECT statements that get executed at query time
--=========================================
-- SELECT STATEMENT 
--=========================================
SELECT  
    formatReadableQuantity(countIf(type = 'terraced')) AS terraced,
    formatReadableQuantity(countIf(type = 'semi-detached')) AS semi_detached,
    formatReadableQuantity(countIf(type = 'detached')) AS detached,
    formatReadableQuantity(countIf(type = 'flat')) AS flat,
    formatReadableQuantity(countIf(type = 'other')) AS other
FROM uk_price_paid
FORMAT Vertical;

--=========================================
-- Create Normal View for above query
--=========================================
CREATE VIEW uk_properties_paid_summary AS 
SELECT  
    formatReadableQuantity(countIf(type = 'terraced')) AS terraced,
    formatReadableQuantity(countIf(type = 'semi-detached')) AS semi_detached,
    formatReadableQuantity(countIf(type = 'detached')) AS detached,
    formatReadableQuantity(countIf(type = 'flat')) AS flat,
    formatReadableQuantity(countIf(type = 'other')) AS other
FROM uk_price_paid;

--=========================================
-- Once this view is created, you can query it like a table
--=========================================
SELECT * FROM uk_properties_paid_summary;

-- EXPERIMENTING WITH different formats
SELECT * FROM uk_properties_paid_summary
FORMAT XML;

SELECT * FROM uk_properties_paid_summary
FORMAT Pretty;

SELECT * FROM uk_properties_paid_summary
FORMAT PrettyCompact;

-- CREATE ANOTHER VIEW
CREATE VIEW uk_terrace_property AS 
SELECT * 
FROM uk_price_paid
WHERE type = 'terraced';

SELECT * FROM uk_terrace_property LIMIT 100;
SELECT count() FROM uk_terrace_property;

--=========================================
-- This is how the VIEW get executed
-- SELECT * FROM uk_terrace_property LIMIT 100;
-- VIEW is inefficient
--=========================================
SELECT count() FROM (
    SELECT * FROM uk_price_paid
    WHERE type = 'terraced'
);
--=========================================
-- When to use Normal view
-- The result of the view change often, which are not great candidates for Materialized views
-- The results of the view are not used very often - relative to the rate at which the result change
-- The query is not resource intensive
-- Every othe scenarios, use Materialised views
--=========================================

--=========================================
-- Materialized view
-- Actual table got created for Materialized view. And this table stores the result of the query
-- Example
--=========================================
SELECT
    max(price) AS max_price
FROM uk_price_paid
WHERE postcode1 = 'DH1' AND postcode2 = '1AD';

