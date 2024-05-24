-- 1378. Replace Employee ID With The Unique Identifier
-- LEFT JOIN
SELECT 
    eu.unique_id, 
    e.name 
FROM 
    employees e
LEFT JOIN 
    employeeUNI eu
ON 
    e.id=eu.id;
-- RIGHT JOIN
SELECT 
    eu.unique_id,
    e.name
FROM 
    employeeUNI eu
RIGHT JOIN
    Employees e
ON 
    e.id = eu.id;

-- 595. Big Countries
SELECT name, population, area 
FROM world
WHERE area >= 3000000 OR (population >= 25000000);

-- 1148. Article Views I
-- Using distinct ensures that each instance of the article is only counted once
-- distinct
```
Input: 
Views table:
+------------+-----------+-----------+------------+
| article_id | author_id | viewer_id | view_date  |
+------------+-----------+-----------+------------+
| 1          | 3         | 5         | 2019-08-01 |
| 1          | 3         | 6         | 2019-08-02 |
| 2          | 7         | 7         | 2019-08-01 |
| 2          | 7         | 6         | 2019-08-02 |
| 4          | 7         | 1         | 2019-07-22 |
| 3          | 4         | 4         | 2019-07-21 |
| 3          | 4         | 4         | 2019-07-21 |
+------------+-----------+-----------+------------+
Output: 
+------+
| id   |
+------+
| 4    |
| 7    |
+------+

```
SELECT DISTINCT (author_id) as id
FROM Views 
WHERE author_id = viewer_id
ORDER BY id;
