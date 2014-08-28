-- START_IGNORE
EXPLAIN
SELECT
 SUM(ct1.c_acctbal) as total_acctbal
FROM TABLESUFFIX_customer ct1
WHERE
 ct1.c_custkey NOT IN(
  SELECT
   DISTINCT ct2.c_custkey
  FROM
   TABLESUFFIX_customer ct2,
   TABLESUFFIX_orders ot
  WHERE
   ct2.c_custkey = ot.o_custkey
  ) a; 
-- END_IGNORE

SELECT
 SUM(ct1.c_acctbal) AS total_acctbal
FROM TABLESUFFIX_customer ct1
WHERE
 ct1.c_custkey NOT IN(
  SELECT
   DISTINCT ct2.c_custkey
  FROM
   TABLESUFFIX_customer ct2,
   TABLESUFFIX_orders ot
  WHERE
   ct2.c_custkey = ot.o_custkey
  ) a;
