-- START_IGNORE
EXPLAIN
SELECT
 AVG(p.p_retailprice * l.l_quantity) as avg_total_price
FROM
 part_TABLESUFFIX_part p,
 lineitem_TABLESUFFIX l
WHERE
 p.p_partkey = l.l_partkey;
-- END_IGNORE

SELECT
 AVG(p.p_retailprice * l.l_quantity) AS avg_total_price
FROM
 part_TABLESUFFIX p,
 lineitem_TABLESUFFIX l
WHERE
 p.p_partkey = l.l_partkey;
