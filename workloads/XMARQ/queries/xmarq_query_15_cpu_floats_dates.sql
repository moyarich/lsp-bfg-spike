SELECT
 COUNT(*)
FROM
 lineitem_TABLESUFFIX
WHERE
 (l_quantity = 1.1e4
  OR l_quantity = 2.1e4
  OR l_quantity = 3.1e4
  OR l_quantity = 4.1e4
  OR l_quantity = 5.1e4
  OR l_quantity = 6.1e4
  OR l_quantity = 7.1e4
  OR l_quantity = 8.1e4
  OR l_quantity = 9.1e4
  OR l_quantity = 50)
 AND (current_date - l_shipdate) > 0
 AND (l_commitdate + 5) < (l_receiptdate + 5)
 AND (l_shipdate + 20) < (l_commitdate + 20);
