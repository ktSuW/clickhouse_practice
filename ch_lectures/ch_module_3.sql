--MODULE 3 Modelling data 
-- CREATE TABLE, DATABASE, DATA TYPES, LOW CARDINALITY , PRIMARY KEY, PARTITION 
-- It is a namespace
CREATE DATABASE my_databse;
-- SHOW TABLES FROM database;
SHOW TABLES FROM system;

-- See columns name and data type of the table
-- DESCRIBE TABLE system.asynchronous_inserts;
DESCRIBE TABLE system.asynchronous_inserts;

-- Give predefined format for a single table's schema.
DESCRIBE TABLE database.table;

-- Offers more flexibility for complex queries and broader scope, allowing you to retrieve column information from multiple tables or databases with customizable output.
-- See table and columns name
SELECT 
    name, 
    type 
FROM 
    system.columns
WHERE 
    database = 'system' 
    AND table = 'asynchronous_inserts';

-- DATA TYPES - 134 types, https://clickhouse.com/docs/en/sql-reference/data-types
-- Built in clickhouse datatypes do not have alias , so alias_to column is empty
-- Starting from row 61. bool 
-- ClickHouse is written in C++
-- Use Decimal for floating point number 

SELECT * 
FROM system.data_type_families;

SELECT 
    * 
FROM 
    system.data_type_families
WHERE 
    alias_to = 'String';

-- DATA TYPES - interesting one in Clickhouse
-- Arrays, Nullable, Enums, LowCardinality
CREATE DATABASE su_datatypes;

CREATE TABLE su_datatypes.travel(
    name String,
    departure_date Date,
    age UInt8,
    city String
)
Engine = MergeTree
PRIMARY Key name;

DESCRIBE TABLE su_datatypes.travel;

ALTER TABLE su_datatypes.travel
    ADD COLUMN appointments Array(DateTime);

SHOW CREATE TABLE su_datatypes.travel;
-- If you run above command, you will see that appointments has been added.
-- Primary key and sort order are the same!
CREATE TABLE su_datatypes.travel
(
    `name` String,
    `departure_date` Date,
    `age` UInt8,
    `city` String,
    `appointments` Array(DateTime)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PRIMARY KEY name
ORDER BY name
SETTINGS index_granularity = 8192

-- INSERT USING ARRAY
-- https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/interval

INSERT INTO su_datatypes.travel VALUES
    ('Pete', '2024-05-22', 32, 'Bali', ['2024-05-24 13:34:00', '1716583836'] ),
    ('Lai', '2024-12-22', 31, 'Puket', ['2024-12-24 13:34:00', '1716593836'] ),
    ('Sakura', '2024-05-22', 51, 'HangZhou', [now(), now() + INTERVAL 1 WEEK] );

SELECT name, appointments FROM su_datatypes.travel;

-- Without using array() function, instead use []
INSERT INTO su_datatypes.travel VALUES
    ('Emily', '2024-06-22', 51, 'Paris', [now(), now() + INTERVAL 1 WEEK, now()  + interval 1 QUARTER ]);
-- use array() function
INSERT INTO su_datatypes.travel VALUES
    ('Zen', '2024-06-01', 21, 'Butan', array(now(), now() + INTERVAL 1 WEEK, now()  + interval 1 QUARTER ));   
-- Array indices are 1-based
-- If that column is empty in the rows you have inserted, the time will show as 1970-01-01 00:00:00
SELECT appointments[3]
FROM su_datatypes.travel;

ALTER TABLE su_datatypes.travel
    ADD COLUMN airticket_price UInt64,
    ADD COLUMN hotel Nullable(UInt64);

DESCRIBE TABLE su_datatypes.travel;

SELECT * FROM su_datatypes.travel;

INSERT INTO su_datatypes.travel (airticket_price, hotel) VALUES
    (1200, 1200),
    (1300, 1300),
    (NULL, 1300000),
    (1400, NULL),
    (1500, 150000);

SELECT * FROM su_datatypes.travel;

-- Updating the existing table
ALTER TABLE su_datatypes.travel 
UPDATE 
    airticket_price = 1200, 
    hotel = 1200 
WHERE 
    name = 'Emily';

-- Delete rows with Null in name
ALTER TABLE su_datatypes.travel
DELETE WHERE name IS NULL;

-- DELETE IS asynchronous
-- is_done = 1: The mutation is completed.
-- is_done = 0: The mutation is still in progress.
SELECT 
    table, 
    is_done 
FROM 
    system.mutations 
WHERE 
    table = 'travel';

SELECT 
    table, 
    command, 
    create_time, 
    is_done 
FROM 
    system.mutations 
WHERE 
    table = 'travel';

SELECT * FROM su_datatypes.travel;

ALTER TABLE su_datatypes.travel 
UPDATE 
    airticket_price = 1200, 
    hotel = 1200 
WHERE 
    name = 'Emily';
---
ALTER TABLE su_datatypes.travel
UPDATE 
    airticket_price = 1400,
    hotel = 1400
WHERE 
    name = 'Lai';

ALTER TABLE su_datatypes.travel
UPDATE 
    airticket_price = 1500,
    hotel = Null
WHERE 
    name = 'Pete';

-- Enum
CREATE TABLE enum_demo(
    device_id UInt32,
    device_type Enum(
        'server' = 1,
        'container' = 2,
        'router' = 3
    )
)
ENGINE = MergeTree()
PRIMARY KEY device_id;

-- Enum column can only contain values in the enum definition
INSERT INTO su_datatypes.enum_demo VALUES 
    (124, 'server'),
    (56743, 2),
    (787, 'container');


ALTER TABLE enum_demo
    ADD COLUMN employee_name String,
    ADD COLUMN commenced_date Date;
    
-- Update row 124
ALTER TABLE su_datatypes.enum_demo
 UPDATE 
    employee_name = 'Sally',
    commenced_date = subtractMonths(today(), 6)
WHERE
    device_id = 124;

-- Update row 787
ALTER TABLE su_datatypes.enum_demo
 UPDATE 
    employee_name = 'Hon',
    commenced_date = today()
WHERE
    device_id = 787;

-- Check the result
SELECT * FROM enum_demo;

ALTER TABLE su_datatypes.enum_demo
UPDATE 
    employee_name = 'Son',
    commenced_date = subtractMonths(today(), 7)
WHERE
    device_id = 56743;

-- ===================  LOW CARDINALITY EXAMPLE ======================
-- 

-- ================== PRIMARY KEY can be defined in several ways ============================
-- it can be inside the column list, 
-- it can be outside the columns
-- ORDER BY 
-- If you want to have different order by, specify them in ORDER BY 
-- Options for creating additional primary indexes
-- Use a projection, hidden table
-- Use a materialized view
-- Define a skipping indexes
-- How to partitions relate to the primary key? - ClickHouse do not use partitions to improve.
-- Partitions is data management.



--====================================================
-- Nullable type cannot be part of the Primary Key
-- Nullable data type adds a hidden column behind the scene, it costs something
-- Use Nullable only if it is needed in business logic

-- Terms/Concepts
-- Table engine, https://clickhouse.com/docs/en/engines/table-engines
-- Merge tree table, must have a primary key
-- LOW CARDINALITY
    -- Better compression 
-- PRIMARY KEY
    -- Must be a prefix of the sorting key
    -- If you query a column frequently, additing it to the primary key means its values are indexed - making for faster query performance
    -- Query execution is significantly more effective and faster on a table where the primary keys columns are sorted by cardinality in ascending order
    -- https://clickhouse.com/docs/en/optimize/sparse-primary-indexes


-- PARTITION by month, date
-- Add column 
-- Array 
-- Interval, https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/interval
-- Enum - 
-- 

