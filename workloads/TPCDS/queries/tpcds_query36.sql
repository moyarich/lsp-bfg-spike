--@author guz4
--@description TPC-DS tpcds_query36
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query36.tpl
select  
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,rank() over (
 	partition by i_category,i_class
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales_TABLESUFFIX
   ,date_dim_TABLESUFFIX       d1
   ,item_TABLESUFFIX
   ,store_TABLESUFFIX
 where
    d1.d_year = 2001 
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk  = ss_item_sk 
 and s_store_sk  = ss_store_sk
 and s_state in ('TN','TN','TN','TN',
                 'TN','TN','TN','TN')
 group by i_category,i_class
 order by
  rank_within_parent
  limit 100;

-- end query 1 in stream 0 using template query36.tpl
