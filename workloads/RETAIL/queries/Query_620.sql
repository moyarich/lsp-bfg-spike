-- Rollup of quantity and price over multiple grains - grouping sets - Q4 2010 to Jan 2011 - Grocery 
select product_category_name
,      case when grouping(to_char(order_datetime, 'YYYY')) || grouping(to_char(order_datetime,'YYYY-Mon')) = '01' then to_char(order_datetime, 'YYYY')
            when grouping(to_char(order_datetime, 'YYYY')) || grouping(to_char(order_datetime,'YYYY-Mon')) = '10' then to_char(order_datetime, 'YYYY-Mon')
            when grouping(to_char(order_datetime, 'YYYY')) || grouping(to_char(order_datetime,'YYYY-Mon')) = '11' then 'Total' 
            else null end as type
,      sum(item_quantity) as total_quantity
,      sum(item_price) as total_price
from   retail_demo.order_lineitems
where  order_Datetime between date '2010/09/01' and date '2011/02/01' 
and    product_id = 49
group by product_category_name
,      grouping sets (product_category_name, to_char(order_datetime, 'YYYY'), to_char(order_datetime,'YYYY-Mon'))
;
