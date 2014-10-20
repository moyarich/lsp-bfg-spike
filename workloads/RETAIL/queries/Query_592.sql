-- Top 10 products for each category for all time
SELECT product_id
,      product_category_id
,      product_count
,      category_rank
FROM (SELECT product_id, product_category_id
      ,      SUM(item_quantity) AS product_count
      ,      row_number() OVER (PARTITION BY product_category_id ORDER BY SUM(item_quantity) DESC) AS category_rank
      FROM   retail_demo.order_lineitems
      GROUP BY product_id, product_category_id
     ) AS lineitems
WHERE category_rank <= 10
ORDER BY product_category_id, category_rank
;
