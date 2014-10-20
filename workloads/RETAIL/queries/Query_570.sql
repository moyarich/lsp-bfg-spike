-- List of customer IDs for customers who have bought Jewelry but have not bought software
SELECT customer_id
FROM  (SELECT customer_id
       ,      SUM(CASE WHEN cat.category_name = 'Jewelry' THEN item_quantity ELSE 0 END) AS bought_quantity
       ,      SUM(CASE WHEN cat.category_name IN ('Digital Software', 'Software') THEN item_quantity ELSE 0 END) AS not_bought_quantity
       FROM   retail_demo.order_lineitems li
       ,      retail_demo.categories_dim cat
       WHERE  li.product_category_id = cat.category_id
       GROUP BY customer_id
      ) AS lineitems
WHERE  bought_quantity > 0
AND    not_bought_quantity = 0
;
