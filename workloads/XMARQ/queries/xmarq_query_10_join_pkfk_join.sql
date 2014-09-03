SELECT
 AVG(p.p_retailprice * l.l_quantity) AS avg_total_price
FROM
 part_TABLESUFFIX p,
 lineitem_TABLESUFFIX l
WHERE
 p.p_partkey = l.l_partkey;
