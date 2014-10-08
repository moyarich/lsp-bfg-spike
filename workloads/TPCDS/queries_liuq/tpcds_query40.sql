--@author guz4
--@description TPC-DS tpcds_query40
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query40.tpl
select  
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2000-03-11' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2000-03-11' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   catalog_sales_TABLESUFFIX left outer join catalog_returns_TABLESUFFIX on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,warehouse_TABLESUFFIX 
  ,item_TABLESUFFIX
  ,date_dim_TABLESUFFIX
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk 
 and cs_sold_date_sk    = d_date_sk
 and d_date between (cast ('2000-03-11' as date) - 30 )
                and (cast ('2000-03-11' as date) + 30 ) 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
limit 100;

-- end query 1 in stream 0 using template query40.tpl
