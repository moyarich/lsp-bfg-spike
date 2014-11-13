-- List of customer IDs for customers who have bought Beauty but have not bought Home
SELECT customer_id
FROM  (SELECT customer_id
       ,      SUM(CASE WHEN cat.category_name IN ('Beauty') THEN item_quantity ELSE 0 END) AS bought_quantity
       ,      SUM(CASE WHEN cat.category_name IN ('Home') THEN item_quantity ELSE 0 END) AS not_bought_quantity
       FROM   retail_demo.order_lineitems li
       ,      retail_demo.categories_dim cat
       WHERE  li.product_category_id = cat.category_id
       GROUP BY customer_id
      ) AS lineitems
WHERE  bought_quantity > 0
AND    not_bought_quantity = 0
;
