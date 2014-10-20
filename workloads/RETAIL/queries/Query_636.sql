-- Orders missing ship SLA for Q3 with YoY
SELECT TO_CHAR(order_datetime, 'YYYY') AS order_year, COUNT(*)
FROM   retail_demo.orders
WHERE  ship_completion_datetime - order_datetime > 4
AND   (order_datetime BETWEEN timestamp '2010-07-01' AND timestamp '2010-09-30 23:59:59'
OR     order_datetime BETWEEN timestamp '2009-07-01' AND timestamp '2009-09-30 23:59:59')
GROUP BY TO_CHAR(order_datetime, 'YYYY')
ORDER  BY TO_CHAR(order_datetime, 'YYYY')
;
