-- START_IGNORE
EXPLAIN
SELECT
 COUNT(DISTINCT l_quantity)
FROM
 lineitem_TABLESUFFIX;
-- END_IGNORE

SELECT
 COUNT(DISTINCT l_quantity)
FROM
 lineitem_TABLESUFFIX;
