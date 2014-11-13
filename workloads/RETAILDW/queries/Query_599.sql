-- Customer first last orders - Toy - redistribute 
select customer_id
,      product_category_name
,      first_order_datetime
,      first_order_id
,      last_order_datetime
,      last_order_id
from  (
select customer_id
,      product_category_name
,      first_value(order_datetime) over (partition by customer_id, product_category_id order by order_datetime asc) as first_order_datetime
,      first_value(order_id) over (partition by customer_id, product_category_id order by order_datetime asc) as first_order_id
,      last_value(order_datetime) over (partition by customer_id, product_category_id order by order_datetime asc) as last_order_datetime
,      last_value(order_id) over (partition by customer_id, product_category_id order by order_datetime asc) as last_order_id
,      row_number() over (partition by customer_id, product_category_id order by null) as rn
from   retail_demo.order_lineitems
where  product_id = 18 
) base
where  base.rn = 1
;
