-- Module 5 - WRITING QUERIES


DESCRIBE TABLE uk_price_paid;

-- CHECK how many rows are in uk_price_paid table
SELECT count(*)
FROM detectionlab.uk_price_paid;

-- Write a count where Street name starts with "Lo"
SELECT COUNT(*)
FROM detectionlab.uk_price_paid
WHERE street LIKE 'L%';

-- WHICH town has the most properties?
SELECT 
    town,
    count() AS number_of_sales
FROM uk_price_paid
GROUP BY town
ORDER BY number_of_sales DESC
LIMIT 15;

-- CHANGE THE OUTPUT FORMAT
-- https://clickhouse.com/docs/en/interfaces/formats
SELECT 
    town,
    count() AS number_of_sales
FROM uk_price_paid
GROUP BY town
ORDER BY number_of_sales DESC
LIMIT 15
-- FORMAT Pretty;
-- FORMAT Vertical;
-- FORMAT JSONEachRow;
-- FORMAT TabSeparatedWithNames;
FORMAT CSVWithNamesAndTypes;

--========= Common Table Expressions =============
-- Define an identifier
-- Identifier is a name given to a database object like a column, table, or aliase. 
-- It is a way to refer to these objects in your queries.
-- Identifiers  - use WITH to define
-- UK_capital is the identifier and its value is LONDON
-- https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions
WITH
    'LONDON' AS UK_captial
SELECT 
    ROUND(avg(price),2) AS avg_price
FROM uk_price_paid
WHERE town = UK_captial;

-- Define a result set
-- A result set in SQL is a set of rows returned by a query.
-- Use CTE to define a result set - allows you to create temporary result sets 
-- that can be referenced in subsequent queries
-- In below example, result  set is most_expensive

-- CTE - A CTE is a temporary result set defined within the execution scope of a single 
-- SELECT, INSERT, UPDATE, or DELETE statement. 
-- It allows you to break down complex queries into simpler parts, improve readability, and reuse the result sets.
WITH most_expensive AS (
    SELECT *
    FROM uk_price_paid
    ORDER BY price DESC
    LIMIT 10
)
SELECT 
    avg(price)
FROM most_expensive;

-- ANOTHER EXAMPLE OF CTE
-- london_properties = CTE selects all properties in LONDON
-- most_expensive = CTE selects the top 5 most expensive properties from london properties
WITH 
london_properties AS 
(
    SELECT * 
    FROM uk_price_paid
    WHERE town = 'LONDON'
),
most_expensive AS 
(
    SELECT * 
    FROM london_properties
    ORDER BY price DESC
    LIMIT 5
)
SELECT 
    avg(price) AS avg_price
FROM most_expensive;

DESCRIBE TABLE uk_price_paid;
-- Calculate Total Transactions in 'MANCHESTER'
WITH
    'MANCHESTER' AS city
SELECT
    ROUND(avg(price), 2) AS manchester_avg_price,
    COUNT(*) AS total_properties
FROM uk_price_paid
WHERE town = city;

-- Top 5 Cheapest Properties in 'GLASGOW'
WITH glasgow_properties AS (
    SELECT *
    FROM uk_price_paid
    WHERE town = 'MANCHESTER'
)
SELECT * 
FROM glasgow_properties
ORDER BY price ASC
LIMIT 5;

-- Functions Categoryies
-- Regular, Aggregate, Table and SQL Window Functions
-- There are at least* two types of functions - regular functions (they are just called “functions”) and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function does not depend on the other rows). 
-- Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

SELECT
    avg(price) OVER(PARTITION BY postcode1),
    *
FROM uk_price_paid
WHERE type = 'terraced'
-- ORDER BY price DESC
AND postcode1 != ''
LIMIT 10;

-- REGULAR FUNCTION - WORKS ON EVERY ROW
-- https://clickhouse.com/docs/en/sql-reference/functions/string-functions
SELECT DISTINCT lower(town)
FROM uk_price_paid
LIMIT 10;

DESCRIBE TABLE uk_price_paid;

-- AGG function returns a single value
SELECT ROUND(avg(price), 2)
FROM uk_price_paid
WHERE town = 'MANCHESTER' 
    AND toYear(date) BETWEEN 2010 AND 2012
    AND toMonth(date) BETWEEN 1 and 12
    AND toDayOfMonth(date) = 1;

SELECT ROUND(avg(price), 2)
FROM uk_price_paid
WHERE town = 'LONDON' 
    AND toYear(date) BETWEEN 2010 AND 2012
    AND toMonth(date) BETWEEN 1 and 12
    AND toDayOfMonth(date) = 1;

SELECT 
    ROUND(avg(price), 2) AS avg_price,
    lower(town)
FROM uk_price_paid
WHERE 
    toYear(date) BETWEEN 2010 AND 2012
    AND toMonth(date) BETWEEN 1 and 12
    AND toDayOfMonth(date) = 1
GROUP BY town 
ORDER BY avg_price DESC 
LIMIT 10;

DESCRIBE uk_price_paid;
-- 
-- HAYSTACK AND NEEDLE
-- Functions in this section also assume that the searched string (referred to in this section as haystack) 
-- and the search string (referred to in this section as needle) are single-byte encoded text. 
-- position - Returns the position (in bytes, starting at 1) of a substring needle in a string haystack.
-- Syntax - position(haystack, needle[, start_pos])
-- > 0 
-- position(street, 'QUEEN') > 0: This condition ensures that only rows where the substring 'QUEEN' appears 
-- in the street column are selected. If position returns a value greater than 0, 
-- it means 'QUEEN' is found in the street string. If position returns 0, 'QUEEN' is not found, and the row is excluded.
SELECT 
    lower(street),
    town,
    price
FROM uk_price_paid
WHERE position(street, 'ORANGE') > 0
ORDER BY price AS DESCENDING
LIMIT 10;


SELECT 
    lower(street),
    town,
    price
FROM uk_price_paid
WHERE multiFuzzyMatchAny(street, 1,['ORANGE']) 
ORDER BY price AS DESCENDING
LIMIT 10;


SELECT DISTINCT
    street,
    multiSearchAllPositionsCaseInsensitive(
        street,
        ['abbey', 'road']
    ) AS positions
FROM uk_price_paid
WHERE NOT has(positions, 0)
LIMIT 10;

SELECT
    max(price),
    toStartOfDay(date) AS day 
FROM uk_price_paid
GROUP BY day
ORDER BY day DESC
LIMIT 10;

WITH now() AS today
SELECT 
    -- today - INTERVAL 1 WEEK;
    today - INTERVAL 1 HOUR;


-- FIND MOST EXPENSIVE HOUSE IN TOWN 
SELECT
    town,
    max(price)
FROM uk_price_paid
GROUP BY town;

--- CTE - OPTION 1
-- FIND MOST EXPENSIVE HOUSE IN TOWN and include the street name of that house
WITH max_prices AS (
    SELECT 
        town,
        max(price) AS max_price
    FROM uk_price_paid
    GROUP BY town
)
SELECT
    uk.town,
    uk.street,
    uk.price
FROM uk_price_paid AS uk
INNER JOIN max_prices AS mp
ON uk.town = mp.town AND uk.price = mp.max_price 
ORDER BY price DESC
LIMIT 10;

--- OPTION 2
-- FIND MOST EXPENSIVE HOUSE IN TOWN and include the street name of that house
-- On which date, sold the most expensive house
-- https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/argmax
SELECT
    town,
    max(price),
    argMax(street, price),
    argMax(date, price)
FROM uk_price_paid
GROUP BY town
LIMIT 10;

-- list of all the functions clickhouse has
SELECT * 
FROM system.functions;

-- Regular functions - incorrect
SELECT * 
FROM system.functions
WHERE is_aggregate= 0 AND is_window_function = 0;

-- Non aggregate or table function markers
SELECT * 
FROM system.functions
WHERE is_aggregate= 0;

-- Aggregate function
SELECT *
FROM system.functions
WHERE is_aggregate = 1;

-- WINDOWS FUNCTION
SELECT name 
FROM system.functions
WHERE name LIKE '%wf%';

-- Table function
SELECT *
FROM system.table_functions;

-- User defined function - need lambda expression as their functional argument
-- arrayMap - take lambda expression 

-- Create a function that you can calll later - SQL UDFs
-- use CREATE FUNCTION with lambda syntax to define your own functions 
-- WIth this, you can use this function just like any ClickHouse function

DESCRIBE TABLE uk_price_paid;
-- SQL User defined function
CREATE FUNCTION mergePostcode AS (p1,p2) -> concat(p1,p2);

SELECT mergePostcode(postcode1, postcode2) 
FROM uk_price_paid;

-- Common aggregate functions
-- count, min, max, sum, avg, median, quantile, quantiles, any (selects the first encountered value), uniqExact, uniqTheta, uniqHLL12
SELECT 
    min(price),
    max(price),
    formatReadableQuantity(sum(price)),
    round(avg(price), 2)
FROM uk_price_paid
FORMAT Vertical;

SELECT quantile( 0.90)(price)
FROM uk_price_paid
WHERE toYear(date) >= '2020';

SELECT quantiles(0.50, 0.80, 0.90)(price)
FROM uk_price_paid
WHERE toYear(date) >= '2020';

SELECT topK(10)(street)
FROM uk_price_paid;

-- AGGREGATE FUNCTION COMBINATORS
-- TERMS CAN BE APPENDED TO ANY AGGREGATE FUNCTIONS
SELECT street, splitByChar(' ', street)
FROM uk_price_paid
LIMIT 10;
