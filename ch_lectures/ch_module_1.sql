-- MODULE 1
SELECT * 
FROM url('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/uk_populations.tsv');

SHOW TABLES IN system;
-- zookeeper
-- zookeeper_connection

-- CREATE TABLE
CREATE TABLE uk_populations_table(
    city LowCardinality(String),
    population UInt32
)
ENGINE = MergeTree
PRIMARY KEY city;

-- INSERT data from tsv file into the table
INSERT INTO uk_populations_table
    SELECT * 
    FROM url('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/uk_populations.tsv');

-- QUERY
SELECT * FROM uk_populations_table;

-- Get column names
SHOW CREATE TABLE uk_populations_table;

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

SELECT * FROM uk_price_paid
LIMIT 100;

-- Get only the headings from the table
SELECT name
FROM system.columns
WHERE 
    database = 'detectionlab' AND 
    table = 'uk_price_paid';

---===============================================================
--- DO THIS EXERCISE
SELECT * FROM common
FROM s3('link of the s3 bucket')
LIMIT 100;


--====================
DESCRIBE s3('');

--====================
-- https://pypistats.org/packages/__all__
SELECT
    PROJECT
    count() AS c
FROM s3('')
GROUP BY PROJECT
ORDER BY c DESC;

SELECT
    toStartOfMonth(TIMESTAMP),
    PROJECT,
    count() AS c
FROM s3('')
GROUP BY month, PROJECT
ORDER BY c DESC;

--====================
SELECT
    avg(price),
    count(),
    town
FROM uk_price_paid
GROUP BY town
-- 1 means most expensive town first 
ORDER BY 1 DESC;
    
-- Study
-- LowCardinality
-- Parque