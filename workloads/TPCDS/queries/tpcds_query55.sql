--@author guz4
--@description TPC-DS tpcds_query55
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query55.tpl
select  i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim_TABLESUFFIX, store_sales_TABLESUFFIX, item_TABLESUFFIX
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=28
 	and d_moy=11
 	and d_year=1999
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
limit 100 ;

-- end query 1 in stream 0 using template query55.tpl
