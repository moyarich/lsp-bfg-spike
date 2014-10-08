--@author guz4
--@description TPC-DS tpcds_query3
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query3.tpl
select  dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum(ss_ext_sales_price) ext_price
 from  date_dim_TABLESUFFIX dt 
      ,store_sales_TABLESUFFIX
      ,item_TABLESUFFIX
 where dt.d_date_sk = store_sales_TABLESUFFIX.ss_sold_date_sk
   and store_sales_TABLESUFFIX.ss_item_sk = item_TABLESUFFIX.i_item_sk
   and item_TABLESUFFIX.i_manufact_id = 128
   and dt.d_moy=11
 group by dt.d_year
      ,item_TABLESUFFIX.i_brand
      ,item_TABLESUFFIX.i_brand_id
 order by dt.d_year
         ,ext_price desc
         ,brand_id
 limit 100;

-- end query 1 in stream 0 using template query3.tpl
