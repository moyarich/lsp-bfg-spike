--@author guz4
--@description TPC-DS tpcds_query24
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query24.tpl
-- modified query to return some results for SF 1
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_paid) netpaid
from store_sales_TABLESUFFIX
    ,store_returns_TABLESUFFIX
    ,store_TABLESUFFIX
    ,item_TABLESUFFIX
    ,customer_TABLESUFFIX
    ,customer_address_TABLESUFFIX
where ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_customer_sk = c_customer_sk
  and ss_item_sk = i_item_sk
  and ss_store_sk = s_store_sk
  and c_birth_country = upper(ca_country)
  and s_zip = ca_zip
  -- modified this qual
  and s_market_id = 9
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales
-- modified this qual
where i_color = 'sienna'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid)
                           from ssales)
 order by 1,2,3,4;

-- end query 1 in stream 0 using template query24.tpl
