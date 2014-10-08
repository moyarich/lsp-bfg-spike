--@author guz4
--@description TPC-DS tpcds_query22
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query22.tpl
select  i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
       from inventory_TABLESUFFIX
           ,date_dim_TABLESUFFIX
           ,item_TABLESUFFIX
           ,warehouse_TABLESUFFIX
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and inv_warehouse_sk = w_warehouse_sk
              and d_year=2000
       group by rollup(i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category)
order by qoh, i_product_name, i_brand, i_class, i_category
limit 100;

-- end query 1 in stream 0 using template query22.tpl
