--@author guz4
--@description TPC-DS tpcds_query82
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query82.tpl
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item_TABLESUFFIX, inventory_TABLESUFFIX, date_dim_TABLESUFFIX, store_sales_TABLESUFFIX
 where i_current_price between 62 and 62+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2000-05-25' as date) and (cast('2000-05-25' as date) +  60 )
 and i_manufact_id in (129,270,821,423)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;

-- end query 1 in stream 0 using template query82.tpl
