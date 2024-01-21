# ClickHouse Studies


## ClickHouse Fundamentals Labs

### Module 1 - Introduction to ClickHouse
- **Lab 2 & 3** 
 
        ```
        -- Create crypto_prices table
        CREATE TABLE crypto_prices 
        (
            trade_date Date,
            crypto_name String,
            volume Float32,
            price Float32,
            market_cap Float32,
            change_1_day Float32
        )
        PRIMARY KEY (crypto_name, trade_date)

        -- Verify the table
        DESC crypto_prices;

        -- Delete the table
        DROP TABLE crypto_prices;

        ```
### Module 2 - Modeling Data
### Module 3 - Inserting Data 
### Module 4 - Analyzing Data
### Module 5 - Views
### Module 6 - Joining Data
### Module 7 - Managing Data
### Module 8 - Optimising ClickHouse

---

## ClickHouse Basic 
<details>
  <summary>Terminologies</summary>

- **Clickhouse Data Types**
    - <ins> Integer types:</ins> signed and unsigned integers (UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256)
    - <ins> Floating-point numbers:</ins> floats(Float32 and Float64) and Decimal values
    - <ins> Boolean:</ins> ClickHouse has a Boolean type
    - <ins> Strings:</ins> String and FixedString
    - <ins> Dates:</ins> use Date and Date32 for days, and DateTime and DateTime64 for instances in time
    - <ins> JSON:</ins> the JSON object stores a JSON document in a single column
    - <ins> UUID:</ins> a performant option for storing UUID values
    - <ins> Low cardinality types:</ins> use an Enum when you have a handful of unique values, or use LowCardinality when you have up to 10,000 unique values of a column
    - <ins> Arrays:</ins> any column can be defined as an Array of values
    - <ins> Maps:</ins> use Map for storing key/value pairs
    - <ins> Aggregation function types:</ins> use SimpleAggregateFunction and AggregateFunction for storing the intermediate status of aggregate function results
    - <ins> Nested data structures:</ins> A Nested data structure is like a table inside a cell
    - <ins> Tuples:</ins> A Tuple of elements, each having an individual type.
    - <ins> Nullable:</ins> Nullable allows you to store a value as NULL when a value is "missing" (instead of the column settings its default value for the data type)
    - <ins> IP addresses:</ins> use IPv4 and IPv6 to efficiently store IP addresses
    - <ins> Geo types:</ins> for geographical data, including Point, Ring, Polygon and MultiPolygon
    - <ins> Special data types:</ins> including Expression, Set, Nothing and Interval
- **Interesting Datatypes**
    - ***Arrays*** - e.g use function ids Array(UInt32) or use square brackets [ ] 
    - ***[Nullable](https://clickhouse.com/docs/en/sql-reference/data-types/nullable)*** - It is not recommended to use Nullable unless you have to for your use case. 
        - If metric is not Nullable, the value would be 0.
        - To store Nullable type values in a table column, ClickHouse uses a separate file with NULL masks in addition to normal file with values. Mask of 0s and 1s. 
        - Whenever you query, that hidden cols get joined with the actual cols.
        - It has overhead both storage and CPU processing.
        - ***IMPORTANT NOTE:*** Using Nullable almost always negatively affects performance, keep this in mind when designing your databases.
    - ***Enums*** - if you have string cols, you can use Enum.
    - ***[LowCardinality](https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality)*** - It is highly recommended to use with strings.
        - Useful when you have a column with a relatively small number of unique values
        - Stores values as integers - use dictionary encoding
        - Advantage over Enums:
            - You can dynamically add new values and you don't need to know all the unique values at the time of table creation 

        ```
            CREATE TABLE lc_t
            (
                `id` UInt16,
                `strings` LowCardinality(String)
            )
            ENGINE = MergeTree()
            ORDER BY id
        ```

- **Database**
    - ***Predefined databases***
        - <ins>default</ins> : Initially empty, it will contain tables that are created witout specifying a database
        - <ins>system</ins> : Contains over 60 system tables that maintain all sorts of details and metadata about your clickhouse deployment
        - <ins>INFORMATION_SCHEMA</ins> : Named after an ANSI standard, this database contains metadata about columns, tables, schemas and views (which are alraedy found in the system database)
- **[Granule](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-data-storage)** - It is a batch of rows of fixed size which addresses with the primary key. The default value is 8,192 rows per batch. 
    - A granule is the smallest indivisible data set that ClickHouse reads when selecting data. ClickHouse does not split rows or values, so each granule always contains an integer number of rows. The first row of a granule is marked with the value of the primary key for the row.
- **MergeTree Table** - tbadded
- **Table Engine** : determins
    - How and where the table data is stored
    - Which queries are supported
    - Concurrent data access
    - Whether multithreaded requests are possible
    - How data is replicated.
    - Use ENGINE clause to specify a table engine 
    - ***Table Engine : Popular speical Table Engines*** that provide a unique and useful purpose
        - <ins>Dictionary</ins> - represent dictionary data as a table
        - <ins> View </ins> - implement views ONLY. It only stores SELECT query, no data
        - <ins> Materialized View </ins> - Stores the actual data from a corresponding SELECT query
        - <ins> File </ins> - Useful for exporting table data to a file or converting data from one format to another (csv, TSV, JSON, XML or more)
        - <ins> URL</ins> - Similar to File, but queries data from a remote HTTP/HTTPs server
        - <ins> Memmory </ins> - Stores data only in memory (data is lost on restart), useful for testing
- **[Partition](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key)**
    - Partitioning is available for the MergeTree family tables, including replicated tables and materialized views.
    - A partition is a logical combination of records in a table by a specified criterion. You can set a partition by an arbitrary criterion, such as by month, by day, or by event type. 
    - Each partition is stored separately to simplify manipulations of this data. When accessing the data, ClickHouse uses the smallest subset of partitions possible
    - Partitions improve performance for queries containing a partitioning key because ClickHouse will filter for that partition before selecting the parts and granules within the partition.
    - **Recommendation :** If you want to improve query performance, focuson defining a good primary key or write a projection or create a materialised view. In most cases, you don't need a partition key. Choose a good clever primary key instead
- **[Primary Key](https://clickhouse.com/docs/en/guides/creating-tables#:~:text=The%20primary%20key%20of%20a,the%20primary%20key%20index%20file.) and Primary Indexes**
    - primary keys in ClickHouse are **not unique** for each row in a table
    - The primary key of a ClickHouse table determines how the data is sorted when written to disk.
    - Every 8,192 rows or 10MB of data (referred to as the index granularity) creates an entry in the primary key index file. This granularity concept creates a sparse index that can easily fit in memory, and the granules represent a stripe of the smallest amount of column data that gets processed during SELECT queries
    - The primary key can be defined using the PRIMARY KEY parameter. If you define a table without a PRIMARY KEY specified, then the key becomes the tuple specified in the ORDER BY clause. If you specify both a PRIMARY KEY and an ORDER BY, the primary key must be a subset of the sort order.
    - All the following Optoin 1, 2 & 3 are the same even though the location of PRIMARY KEY() is different.
    - Syntax Options
        - Option 1 - Defining inside the coloum list

            ```
            CREATE TABLE option1_table
            (
                use_id UInt32,
                message String,
                timestamp DateTime
                metric Decimal(30,2)
                PRIMARY KEY (user_id, timestamp)
            )
            ENGINE = MergeTree
            ```
        - Option 2 - Defining PRIMARY KEY after the TABLE ENGINE

            ```
            CREATE TABLE option2_table
            (
                use_id UInt32,
                message String,
                timestamp DateTime
                metric Decimal(30,2)
            )
            ENGINE = MergeTree
            PRIMARY KEY (user_id, timestamp)
            ```
        - Option 3 - you can have primary key and sort order that are different, but there must be some consistency

            ```
            CREATE TABLE option3_table
            (
                use_id UInt32,
                message String,
                timestamp DateTime
                metric Decimal(30,2)
                PRIMARY KEY(user_id, timestamp)
            )
            ENGINE = MergeTree
            ORDER BY (user_id, timestamp, message)

            ```
    - **Good candidates for primary key columns**
        - lots of queries on a column - if you query a column frequently, adding it to the primary key means its values are indexed and making for faster query performance
        - Order by cardinality in ascedning order, LowCardinality first
    - **Options for creating additional primary indexes**
        - ***Create two tables for the same data*** And the second table with a different primary key 
        - ***Use a projection*** : you can use a single table, but clickhouse creates a hidden table that stores the data sorted in a different day 
        - ***Use a materialized view*** : Stores a data in a separate table based on a SELECT statement, sort the data in the SELECT statement
        - ***Define a skipping index***
    - Further reading
        - [How Clickhouse primary key works and how to choose it](https://medium.com/datadenys/how-clickhouse-primary-key-works-and-how-to-choose-it-4aaf3bf4a8b9)
        - [A Practical Introduction to Primary Indexes in ClickHouse](https://clickhouse.com/docs/en/optimize/sparse-primary-indexes)

## SQL and other fundamentals concepts for ClickHouse

- Cardinality
- Multithreading and Multithreaded request
- Namespace
- Partitioning
- Primary Key 
- UnsignedInt vs SignedInt

</details>