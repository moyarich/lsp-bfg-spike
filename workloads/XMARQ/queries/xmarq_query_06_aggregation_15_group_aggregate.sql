-- START_IGNORE
EXPLAIN
SELECT
 o_orderstatus,
 o_orderpriority,
 AVG(o_totalprice) AS ave_o_totalprice
FROM
  orders_TABLESUFFIX
GROUP BY
 o_orderstatus,
 o_orderpriority;
-- END_IGNORE

SELECT
 o_orderstatus,
 o_orderpriority,
 AVG(o_totalprice) AS avg_o_totalprice
FROM
 orders_TABLESUFFIX
GROUP BY
 o_orderstatus,
 o_orderpriority;
