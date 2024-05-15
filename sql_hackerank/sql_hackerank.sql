```
584.Find customer reference
```
select name from Customer
where referee_id != 2 or referee_id is null;

```
1757. Recyclable and Low Fat Products
```
select product_id
from products
where low_fats='y' and recyclable='y';

```
1378. Replace Employee ID With The Unique Identifier
```
-- Employees table : id, name
-- EmployeeUNI : id, unique_id
-- Output - unique_id, name
SELECT eu.unique_id, e.name 
FROM Employees AS e
LEFT JOIN EmployeeUNI AS eu
ON e.id = eu.id;


```
Query all columns for all American cities in the CITY table with populations larger than 100000. The CountryCode for America is USA.
The CITY table is described as follows:
```
SELECT *
FROM CITY
WHERE POPULATION > 100000 AND 
    COUNTRYCODE = "USA";

```
Query the NAME field for all American cities in the CITY table with populations larger than 120000. The CountryCode for America is USA.
The CITY table is described as follows:
```
SELECT NAME
FROM CITY
WHERE POPULATION > 100000 AND 
    COUNTRYCODE = "USA";

--- Weather observation station - 1
SELECT CITY, STATE
FROM STATION;

-- Weather station - 6
SELECT DISTINCT CITY
FROM STATION
WHERE CITY REGEXP '^[AEIOUaeiou]';