SELECT
 AVG(l.l_quantity) AS avg_quantity
FROM
 lineitem_TABLESUFFIX l,
 orders_TABLESUFFIX o
WhERE
 l.l_orderkey = o.o_orderkey;