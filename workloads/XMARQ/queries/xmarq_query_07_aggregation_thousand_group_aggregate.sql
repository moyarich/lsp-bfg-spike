-- START_IGNORE
EXPLAIN
SELECT
 l_receiptdate,
 COUNT(*) AS num
FROM
 TABLESUFFIX_lineitem
GROUP BY
 l_receiptdate
ORDER BY
 l_receiptdate;
-- EDN_IGNORE

SELECT
 l_receiptdate,
 COUNT(*) AS num
FROM
 TABLESUFFIX_lineitem
GROUP BY
 l_receiptdate
ORDER BY
 l_receiptdate;
