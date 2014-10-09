--@author guz4
--@description TPC-DS tpcds_query73
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query73.tpl
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag 
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales_TABLESUFFIX,date_dim_TABLESUFFIX,store_TABLESUFFIX,household_demographics_TABLESUFFIX
    where store_sales_TABLESUFFIX.ss_sold_date_sk = date_dim_TABLESUFFIX.d_date_sk
    and store_sales_TABLESUFFIX.ss_store_sk = store_TABLESUFFIX.s_store_sk  
    and store_sales_TABLESUFFIX.ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
    and date_dim_TABLESUFFIX.d_dom between 1 and 2 
    and (household_demographics_TABLESUFFIX.hd_buy_potential = '>10000' or
         household_demographics_TABLESUFFIX.hd_buy_potential = 'unknown')
    and household_demographics_TABLESUFFIX.hd_vehicle_count > 0
    and case when household_demographics_TABLESUFFIX.hd_vehicle_count > 0 then 
             household_demographics_TABLESUFFIX.hd_dep_count/ household_demographics_TABLESUFFIX.hd_vehicle_count else null end > 1
    and date_dim_TABLESUFFIX.d_year in (1999,1999+1,1999+2)
    and store_TABLESUFFIX.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dj,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
    order by cnt desc;

-- end query 1 in stream 0 using template query73.tpl
