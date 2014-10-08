--@author guz4
--@description TPC-DS tpcds_query16
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query16.tpl
select  
   count(distinct cs_order_number) as "order count"
  ,sum(cs_ext_ship_cost) as "total shipping cost"
  ,sum(cs_net_profit) as "total net profit"
from
   catalog_sales_TABLESUFFIX cs1
  ,date_dim_TABLESUFFIX
  ,customer_address_TABLESUFFIX
  ,call_center_TABLESUFFIX
where
    d_date between '2002-2-01' and 
           (cast('2002-2-01' as date) + 60 )
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'GA'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
                  'Williamson County'
)
and exists (select *
            from catalog_sales_TABLESUFFIX cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from catalog_returns_TABLESUFFIX cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
limit 100;

-- end query 1 in stream 0 using template query16.tpl
