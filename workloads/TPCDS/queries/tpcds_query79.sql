--@author guz4
--@description TPC-DS tpcds_query79
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query79.tpl
select 
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store_TABLESUFFIX.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales_TABLESUFFIX,date_dim_TABLESUFFIX,store_TABLESUFFIX,household_demographics_TABLESUFFIX
    where store_sales_TABLESUFFIX.ss_sold_date_sk = date_dim_TABLESUFFIX.d_date_sk
    and store_sales_TABLESUFFIX.ss_store_sk = store_TABLESUFFIX.s_store_sk  
    and store_sales_TABLESUFFIX.ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
    and (household_demographics_TABLESUFFIX.hd_dep_count = 6 or household_demographics_TABLESUFFIX.hd_vehicle_count > 2)
    and date_dim_TABLESUFFIX.d_dow = 1
    and date_dim_TABLESUFFIX.d_year in (1999,1999+1,1999+2) 
    and store_TABLESUFFIX.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store_TABLESUFFIX.s_city) ms,customer_TABLESUFFIX
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
limit 100;

-- end query 1 in stream 0 using template query79.tpl
