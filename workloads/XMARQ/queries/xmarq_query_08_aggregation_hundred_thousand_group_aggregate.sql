-- START_IGNORE
EXPLAIN
SELECT
 SUBSTRING(o_comment,from 1 for 15) AS substring_comment,
 COUNT(*) AS num
FROM
 orders_TABLESUFFIX
GROUP BY
 substring_comment;
-- END_IGNORE

SELECT
 SUBSTRING(o_comment,from 1 for 15) AS substring_comment,
 COUNT(*) AS num
FROM
 orders_TABLESUFFIX
GROUP BY
 substring_comment;
