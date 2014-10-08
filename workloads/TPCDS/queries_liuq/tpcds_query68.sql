--@author guz4
--@description TPC-DS tpcds_query68
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query68.tpl
select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,extended_tax
       ,list_price
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price 
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax 
       from store_sales_TABLESUFFIX
           ,date_dim_TABLESUFFIX
           ,store_TABLESUFFIX
           ,household_demographics_TABLESUFFIX
           ,customer_address_TABLESUFFIX 
       where store_sales_TABLESUFFIX.ss_sold_date_sk = date_dim_TABLESUFFIX.d_date_sk
         and store_sales_TABLESUFFIX.ss_store_sk = store_TABLESUFFIX.s_store_sk  
        and store_sales_TABLESUFFIX.ss_hdemo_sk = household_demographics_TABLESUFFIX.hd_demo_sk
        and store_sales_TABLESUFFIX.ss_addr_sk = customer_address_TABLESUFFIX.ca_address_sk
        and date_dim_TABLESUFFIX.d_dom between 1 and 2 
        and (household_demographics_TABLESUFFIX.hd_dep_count = 4 or
             household_demographics_TABLESUFFIX.hd_vehicle_count= 3)
        and date_dim_TABLESUFFIX.d_year in (1999,1999+1,1999+2)
        and store_TABLESUFFIX.s_city in ('Fairview','Midway')
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city) dn
      ,customer_TABLESUFFIX
      ,customer_address_TABLESUFFIX current_addr
 where ss_customer_sk = c_customer_sk
   and customer_TABLESUFFIX.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
 order by c_last_name
         ,ss_ticket_number
 limit 100;

-- end query 1 in stream 0 using template query68.tpl
