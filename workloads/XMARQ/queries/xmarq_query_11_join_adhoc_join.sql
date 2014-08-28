-- START_IGNORE
EXPLAIN
SELECT
 SUM(a.l_quantity) as total_quantity
FROM
 TABLESUFFIX_part a,
 TABLESUFFIX_lineitem b
WHERE
 a.p_partkey = b.l_partkey;
-- END_IGNORE

SELECT
 SUM(a.l_quantity) as total_quantity
FROM
 TABLESUFFIX_part a,
 TABLESUFFIX_lineitem b
WHERE
 a.p_partkey = b.l_partkey;
