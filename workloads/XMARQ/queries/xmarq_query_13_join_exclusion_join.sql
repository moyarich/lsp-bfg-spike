-- START_IGNORE
EXPLAIN
SELECT
 SUM(c1.c_acctbal) as total_acctbal
FROM customer_TABLESUFFIX c1
WHERE
 c1.c_custkey NOT IN(
  SELECT
   DISTINCT c2.c_custkey
  FROM
   customer_TABLESUFFIX c2,
   orders_TABLESUFFIX o
  WHERE
   c2.c_custkey = o.o_custkey
  ) a; 
-- END_IGNORE

SELECT
 SUM(c1.c_acctbal) AS total_acctbal
FROM customer_TABLESUFFIX c1
WHERE
 c1.c_custkey NOT IN(
  SELECT
   DISTINCT c2.c_custkey
  FROM
   customer_TABLESUFFIX c2,
   orders_TABLESUFFIX o
  WHERE
   c2.c_custkey = o.o_custkey
  ) a;
