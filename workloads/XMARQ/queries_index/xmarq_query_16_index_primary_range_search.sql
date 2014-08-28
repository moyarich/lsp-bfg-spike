-- START_IGNORE
EXPLAIN
SELECT
 p_name,
 p_retailprice
FROM
 TABLESUFFIX_part
WHERE
 p_partkey < 5000
 AND p_retailprice < 909.00;
-- END_IGNORE

SELECT
 p_name,
 p_retailprice
FROM
 TABLESUFFIX_part
WHERE
 p_partkey < 5000
 AND p_retailprice < 909.00;
