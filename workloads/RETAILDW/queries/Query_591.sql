-- Number of orders by shipment status in 2010
SELECT item_shipment_status_code
,      COUNT(DISTINCT order_id) AS num_orders
FROM   retail_demo.order_lineitems
WHERE  order_datetime BETWEEN timestamp '2010-01-01' AND date '2010-12-31'
GROUP BY item_shipment_status_code
ORDER BY item_shipment_status_code
;
