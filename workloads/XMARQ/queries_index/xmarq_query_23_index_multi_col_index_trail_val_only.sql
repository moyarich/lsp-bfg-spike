-- START_IGNORE
EXPLAIN
SELECT
 AVG(p_retailprice)
FROM
 TABLESUFFIX_part
WHERE
 p_size = 21;
-- END_IGNORE

SELECT
 AVG(p_retailprice)
FROM
 TABLESUFFIX_part
WHERE
 p_size = 21;
