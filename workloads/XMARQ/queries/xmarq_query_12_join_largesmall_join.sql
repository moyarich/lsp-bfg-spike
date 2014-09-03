SELECT
 n.n_name,
 AVG(c.c_acctbal) AS avg_acctbal
FROM
 customer_TABLESUFFIX c,
 nation_TABLESUFFIX n
WHERE
 c.c_nationkey = n.n_nationkey
GROUP BY
 n.n_name;
