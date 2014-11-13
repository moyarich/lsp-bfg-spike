 -- Average time to ship for orders in Digital Music Album - 2010-09-01 + YA (year ago) 
 select c.category_name 
 ,      avg(case when o.order_datetime between timestamp '2010-09-01' and date '2010-09-01' + 1 then o.ship_datetime - o.order_datetime else null end) as avg_time_to_ship 
 ,      avg(case when o.order_datetime between timestamp '2010-09-01' - '366 Day'::INTERVAL  and date '2010-09-01' - 365 then o.ship_datetime - o.order_datetime else null end) as ya_avg_time_to_ship 
 ,      sum(case when o.order_datetime between timestamp '2010-09-01' and date '2010-09-01' + 1 then o.item_quantity else null end) as total_quantity 
 ,      sum(case when o.order_datetime between timestamp '2010-09-01' - '366 Day'::INTERVAL  and date '2010-09-01' - 365 then o.item_quantity else null end) as ya_total_quantity 
 ,      count(distinct case when o.order_datetime between timestamp '2010-09-01' and date '2010-09-01' + 1 then o.product_id else null end) as product_cnt 
 ,      count(distinct case when o.order_datetime between timestamp '2010-09-01' - '366 Day'::INTERVAL  and date '2010-09-01' - 365 then o.product_id else null end) as ya_product_cnt 
 ,      count(distinct case when o.order_datetime between timestamp '2010-09-01' and date '2010-09-01' + 1 then o.order_id else null end) as total_orders 
 ,      count(distinct case when o.order_datetime between timestamp '2010-09-01' - '366 Day'::INTERVAL  and date '2010-09-01' - 365 then o.order_id else null end) as ya_total_orders 
 from   retail_demo.order_lineitems o 
 ,      retail_demo.products_dim p 
 ,      retail_demo.categories_dim c 
 where o.product_id = p.product_id 
 and    o.ship_datetime is not null 
 and    p.category_id = c.category_id 
 and    p.category_id = 25
 and   (o.order_datetime between timestamp '2010-09-01' and date '2010-09-01' + 1 
  or    o.order_datetime between timestamp '2010-09-01' - '366 Day'::INTERVAL  and date '2010-09-01' - 365)
 group by c.category_name 
 
;
