--@author guz4
--@description TPC-DS tpcds_query97
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query97.tpl
with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales_TABLESUFFIX,date_dim_TABLESUFFIX
where ss_sold_date_sk = d_date_sk
  and d_year=2000
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales_TABLESUFFIX,date_dim_TABLESUFFIX
where cs_sold_date_sk = d_date_sk
  and d_year=2000
group by cs_bill_customer_sk
        ,cs_item_sk)
 select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;

-- end query 1 in stream 0 using template query97.tpl
