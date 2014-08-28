-- START_IGNORE
EXPLAIN
SELECT
 AVG(a.l_quantity) AS avg_quantity
FROM
 TABLESUFFIX_lineitem a,
 TABLESUFFIX_orders b
WHERE
 a.l_orderkey = b.o_orderkey;
-- END_IGNORE

SELECT
 AVG(a.l_quantity) AS avg_quantity
FROM
 TABLESUFFIX_lineitem a,
 TABLESUFFIX_orders b
WhERE
 a.l_orderkey = b.o_orderkey;


