-- Items sold by category filtered for categories in July with more than 100000 total quantity
SELECT product_category_id
,      SUM(item_quantity) AS category_item_count
FROM   retail_demo.order_lineitems
WHERE  order_datetime BETWEEN timestamp '2010-07-01' AND date '2010-07-31'
GROUP BY product_category_id
HAVING SUM(item_quantity) > 100000
ORDER BY category_item_count
;
