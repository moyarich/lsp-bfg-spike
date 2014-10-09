--@author guz4
--@description TPC-DS tpcds_query88
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query88.tpl
select  *
from
 (select count(*) h8_30_to_9
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk   
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and time_dim_TABLESUFFIX.t_hour = 8
     and time_dim_TABLESUFFIX.t_minute >= 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2)) 
     and store_TABLESUFFIX.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim_TABLESUFFIX.t_hour = 9 
     and time_dim_TABLESUFFIX.t_minute < 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2))
     and store_TABLESUFFIX.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim_TABLESUFFIX.t_hour = 9
     and time_dim_TABLESUFFIX.t_minute >= 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2))
     and store_TABLESUFFIX.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim_TABLESUFFIX.t_hour = 10 
     and time_dim_TABLESUFFIX.t_minute < 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2))
     and store_TABLESUFFIX.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim_TABLESUFFIX.t_hour = 10 
     and time_dim_TABLESUFFIX.t_minute >= 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2))
     and store_TABLESUFFIX.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim_TABLESUFFIX.t_hour = 11
     and time_dim_TABLESUFFIX.t_minute < 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2))
     and store_TABLESUFFIX.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim_TABLESUFFIX.t_hour = 11
     and time_dim_TABLESUFFIX.t_minute >= 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2))
     and store_TABLESUFFIX.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from store_sales_TABLESUFFIX, household_demographics_TABLESUFFIX , time_dim_TABLESUFFIX, store_TABLESUFFIX
 where ss_sold_time_sk = time_dim_TABLESUFFIX.t_time_sk
     and ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim_TABLESUFFIX.t_hour = 12
     and time_dim_TABLESUFFIX.t_minute < 30
     and ((household_demographics_TABLESUFFIX.hd_dep_count = 4 and household_demographics_TABLESUFFIX.hd_vehicle_count<=4+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 2 and household_demographics_TABLESUFFIX.hd_vehicle_count<=2+2) or
          (household_demographics_TABLESUFFIX.hd_dep_count = 0 and household_demographics_TABLESUFFIX.hd_vehicle_count<=0+2))
     and store_TABLESUFFIX.s_store_name = 'ese') s8
;

-- end query 1 in stream 0 using template query88.tpl
