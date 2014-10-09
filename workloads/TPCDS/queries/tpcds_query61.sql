--@author guz4
--@description TPC-DS tpcds_query61
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query61.tpl
select  promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales_TABLESUFFIX
        ,store_TABLESUFFIX
        ,promotion_TABLESUFFIX
        ,date_dim_TABLESUFFIX
        ,customer_TABLESUFFIX
        ,customer_address_TABLESUFFIX 
        ,item_TABLESUFFIX
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk 
   and   ca_gmt_offset = -7
   and   i_category = 'Jewelry'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -7
   and   d_year = 1998
   and   d_moy  = 11) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  store_sales_TABLESUFFIX
        ,store_TABLESUFFIX
        ,date_dim_TABLESUFFIX
        ,customer_TABLESUFFIX
        ,customer_address_TABLESUFFIX
        ,item_TABLESUFFIX
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7
   and   i_category = 'Jewelry'
   and   s_gmt_offset = -7
   and   d_year = 1998
   and   d_moy  = 11) all_sales
order by promotions, total
limit 100;

-- end query 1 in stream 0 using template query61.tpl
