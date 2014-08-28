-- START_IGNORE
EXPLAIN
SELECT
 l_discount,
 COUNT(*)
FROM
 TABLESUFFIX_lineitem
GROUP BY
 l_discount;
-- END_IGNORE

SELECT
 l_discount,
 COUNT(*)
FROM
 TABLESUFFIX_lineitem
GROUP BY
 l_discount;
