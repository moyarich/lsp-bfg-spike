-- START_IGNORE
EXPLAIN
SELECT
 AVG(a.l_quantity) AS avg_quantity
FROM
 lineitem_TABLESUFFIX l,
 orders_TABLESUFFIX o
WHERE
 l.l_orderkey = o.o_orderkey;
-- END_IGNORE

SELECT
 AVG(a.l_quantity) AS avg_quantity
FROM
 lineitem_TABLESUFFIX l,
 orders_TABLESUFFIX o
WhERE
 l.l_orderkey = o.o_orderkey;


