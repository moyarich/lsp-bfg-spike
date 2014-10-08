--@author guz4
--@description TPC-DS tpcds_query15
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query15.tpl
select  ca_zip
       ,sum(cs_sales_price)
 from catalog_sales_TABLESUFFIX
     ,customer_TABLESUFFIX
     ,customer_address_TABLESUFFIX
     ,date_dim_TABLESUFFIX
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2001
 group by ca_zip
 order by ca_zip
 limit 100;

-- end query 1 in stream 0 using template query15.tpl
