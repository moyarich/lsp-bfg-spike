-- Count of orders, items, and customers ordering for Q3-2010
SELECT COUNT(DISTINCT order_id) AS num_orders
,      SUM(item_quantity) AS num_items
,      COUNT(DISTINCT customer_id) AS num_customers
FROM   retail_demo.order_lineitems
WHERE  order_datetime BETWEEN timestamp '2010-07-01' AND date '2010-09-30'
;
