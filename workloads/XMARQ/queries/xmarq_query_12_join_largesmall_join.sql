-- START_IGNORE
EXPLAIN
SELECT
 nt.n_name,
 AVG(ct.c_acctbal) AS avg_acctbal
FROM
 TABLESUFFIX_customer ct,
 TABLESUFFIX_nation nt
WHERE
 ct.c_nationkey = nt.n_nationkey
GROUP BY
 nt.n_name;
-- END_IGNORE

SELECT
 nt.n_name,
 AVG(ct.c_acctbal) AS avg_acctbal
FROM
 TABLESUFFIX_customer ct,
 TABLESUFFIX_nation nt
WHERE
 ct.c_nationkey = nt.n_nationkey
GROUP BY
 nt.n_name;
