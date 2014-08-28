-- START_IGNORE
EXPLAIN
SELECT
 l_orderkey,
 l_linenumber
FROM
 TABLESUFFIX_lineitem
WHERE
 l_shipdate < 981200;
-- END_IGNORE

SELECT
 l_orderkey,
 l_linenumber
FROM
 TABLESUFFIX_lineitem
WHERE
 l_shipdate < 981200;

