-- Items sold by category filtered for categories selling over 100,000 items
SELECT product_category_id
,      SUM(item_quantity) AS category_item_count
FROM   retail_demo.order_lineitems
WHERE  order_datetime BETWEEN timestamp '2010-01-01' AND date '2010-01-31'
GROUP BY product_category_id
HAVING SUM(item_quantity) > 100000
ORDER BY category_item_count
;
