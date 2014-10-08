--@author guz4
--@description TPC-DS tpcds_query32
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query32.tpl
select  sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   catalog_sales_TABLESUFFIX 
   ,item_TABLESUFFIX 
   ,date_dim_TABLESUFFIX
where
i_manufact_id = 977
and i_item_sk = cs_item_sk 
and d_date between '2000-01-27' and 
        (cast('2000-01-27' as date) + 90 )
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '2000-01-27' and
                             (cast('2000-01-27' as date) + 90 )
          and d_date_sk = cs_sold_date_sk 
      ) 
limit 100;

-- end query 1 in stream 0 using template query32.tpl
