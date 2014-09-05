SELECT
 l_returnflag,
 l_linestatus,
 l_shipmode,
 SUBSTRING(l_shipinstruct, 1, 1),
 SUBSTRING(l_linestatus, 1, 1),
 ((l_quantity - l_linenumber) + (l_linenumber - l_quantity)),
 (l_extendedprice - l_extendedprice),
 SUM((1 + l_tax) * l_extendedprice),
 SUM((1 - l_discount) * l_extendedprice),
 SUM(l_discount / 3),
 SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
 SUM(l_extendedprice - ((1- l_discount) * l_extendedprice)),
 SUM(current_date - l_shipdate + 5),
 SUM(l_shipdate - l_commitdate),
 SUM(l_receiptdate - l_shipdate),
 SUM(l_linenumber + 15 - 14),
 SUM(l_extendedprice / (10 - l_tax)),
 SUM((l_quantity * 2) / (l_linenumber * 3)),
 COUNT(*)
FROM
 lineitem_TABLESUFFIX
WHERE
 l_linenumber > 2
GROUP BY
 l_returnflag,
 l_linestatus,
 l_shipmode,
 SUBSTRING(l_shipinstruct, 1, 1),
 SUBSTRING(l_linestatus, 1, 1),
 ((l_quantity - l_linenumber) + (l_linenumber - l_quantity)),
 (l_extendedprice - l_extendedprice);
