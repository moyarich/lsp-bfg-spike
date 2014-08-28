-- START_IGNORE
EXPLAIN
SELECT
 SUM(l.l_quantity) as total_quantity
FROM
 part_TABLESUFFIX p,
 lineitem_TABLESUFFIX l
WHERE
 p.p_partkey = l.l_partkey;
-- END_IGNORE

SELECT
 SUM(l.l_quantity) as total_quantity
FROM
 part_TABLESUFFIX p,
 lineitem_TABLESUFFIX l
WHERE
 p.p_partkey = l.l_partkey;
