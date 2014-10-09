--@author guz4
--@description TPC-DS tpcds_query92
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query92.tpl
select  
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    web_sales_TABLESUFFIX 
   ,item_TABLESUFFIX 
   ,date_dim_TABLESUFFIX
where
i_manufact_id = 977
and i_item_sk = ws_item_sk 
and d_date between '2000-01-27' and 
        (cast('2000-01-27' as date) + 90 )
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            web_sales_TABLESUFFIX 
           ,date_dim_TABLESUFFIX
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '2000-01-27' and
                             (cast('2000-01-27' as date) + 90)
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
limit 100;

-- end query 1 in stream 0 using template query92.tpl
