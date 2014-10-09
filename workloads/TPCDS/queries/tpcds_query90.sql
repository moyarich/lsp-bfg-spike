--@author guz4
--@description TPC-DS tpcds_query90
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query90.tpl
select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, web_page_TABLESUFFIX
       where ws_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
         and ws_ship_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
         and ws_web_page_sk = web_page_TABLESUFFIX.wp_web_page_sk
         and time_dim_TABLESUFFIX.t_hour between 8 and 8+1
         and household_demographics_TABLESUFFIX.hd_dep_count = 6
         and web_page_TABLESUFFIX.wp_char_count between 5000 and 5200) at,
      ( select count(*) pmc
       from web_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, web_page_TABLESUFFIX
       where ws_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
         and ws_ship_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
         and ws_web_page_sk = web_page_TABLESUFFIX.wp_web_page_sk
         and time_dim_TABLESUFFIX.t_hour between 19 and 19+1
         and household_demographics_TABLESUFFIX.hd_dep_count = 6
         and web_page_TABLESUFFIX.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100;

-- end query 1 in stream 0 using template query90.tpl
