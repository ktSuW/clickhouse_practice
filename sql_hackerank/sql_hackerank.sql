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