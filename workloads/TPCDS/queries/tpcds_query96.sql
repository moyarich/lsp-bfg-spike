--@author guz4
--@description TPC-DS tpcds_query96
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query96.tpl
select  count(*) 
from store_sales_TABLESUFFIX
    ,household_demographics_TABLESUFFIX 
    ,time_dim_TABLESUFFIX, store_TABLESUFFIX
where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk   
    and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and time_dim_TABLESUFFIX.t_hour = 20
    and time_dim_TABLESUFFIX.t_minute >= 30
    and household_demographics_TABLESUFFIX.hd_dep_count = 7
    and store_TABLESUFFIX.s_store_name = 'ese'
order by count(*)
limit 100;

-- end query 1 in stream 0 using template query96.tpl
