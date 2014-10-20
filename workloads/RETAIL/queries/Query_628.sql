-- Top ten products by quantity sold for Jul vs. YoY
SELECT order_month, total_sold, product_name, item_rank
FROM  (SELECT TO_CHAR(order_datetime, 'YYYY-MM') AS order_month
       ,      product_id
       ,      SUM(item_quantity) AS total_sold
       ,      row_number() OVER (PARTITION BY TO_CHAR(order_datetime, 'YYYY-MM') ORDER BY SUM(item_quantity)) AS item_rank
       FROM   retail_demo.order_lineitems
       WHERE (order_datetime BETWEEN timestamp '2010-07-01' AND timestamp '2010-07-30 23:59:59'
       OR     order_datetime BETWEEN timestamp '2009-07-01' AND timestamp '2009-07-30 23:59:59')
       GROUP BY TO_CHAR(order_datetime, 'YYYY-MM'), product_id
      ) AS lineitems
,      retail_demo.products_dim p
WHERE  p.product_id = lineitems.product_id
AND    lineitems.item_rank <= 10
ORDER BY item_rank, order_month
;
