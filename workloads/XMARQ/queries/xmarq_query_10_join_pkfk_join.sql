-- START_IGNORE
EXPLAIN
SELECT
 AVG(a.p_retailprice * b.l_quantity) as avg_total_price
FROM
 partTABLESUFFIX_part a,
 lineitemTABLESUFFIX_lineitem b
WHERE
 partTABLESUFFIa.p_partkey = lineitemTABLESUFFIb.l_partkey;
-- END_IGNORE

SELECT
 AVG(a.p_retailprice * b.l_quantity) AS avg_total_price
FROM
 TABLESUFFIX_part a,
 TABLESUFFIX_lineitem b
WHERE
 a.p_partkey = b.l_partkey;
