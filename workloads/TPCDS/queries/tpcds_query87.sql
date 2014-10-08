--@author guz4
--@description TPC-DS tpcds_query87
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query87.tpl
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales_TABLESUFFIX, date_dim_TABLESUFFIX, customer_TABLESUFFIX
       where store_sales_TABLESUFFIX.ss_sold_date_sk = date_dim_TABLESUFFIX.d_date_sk
         and store_sales_TABLESUFFIX.ss_customer_sk = customer_TABLESUFFIX.c_customer_sk
         and d_year = 2000)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales_TABLESUFFIX, date_dim_TABLESUFFIX, customer_TABLESUFFIX
       where catalog_sales_TABLESUFFIX.cs_sold_date_sk = date_dim_TABLESUFFIX.d_date_sk
         and catalog_sales_TABLESUFFIX.cs_bill_customer_sk = customer_TABLESUFFIX.c_customer_sk
         and d_year = 2000)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales_TABLESUFFIX, date_dim_TABLESUFFIX, customer_TABLESUFFIX
       where web_sales_TABLESUFFIX.ws_sold_date_sk = date_dim_TABLESUFFIX.d_date_sk
         and web_sales_TABLESUFFIX.ws_bill_customer_sk = customer_TABLESUFFIX.c_customer_sk
         and d_year = 2000)
) cool_cust
;

-- end query 1 in stream 0 using template query87.tpl
