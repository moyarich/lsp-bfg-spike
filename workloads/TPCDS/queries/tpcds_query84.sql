--@author guz4
--@description TPC-DS tpcds_query84
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query84.tpl
select  c_customer_id as customer_id ,c_last_name || ', ' || c_first_name as customername
from customer_TABLESUFFIX, customer_address_TABLESUFFIX ,customer_demographics_TABLESUFFIX ,household_demographics_TABLESUFFIX ,income_band_TABLESUFFIX ,store_returns_TABLESUFFIX
 where ca_city = 'Edgewood' and c_current_addr_sk = ca_address_sk and ib_lower_bound   >=  38128 and ib_upper_bound <=  38128 + 50000 and ib_income_band_sk = hd_income_band_sk 
and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and sr_cdemo_sk = cd_demo_sk order by c_customer_id limit 100;
-- end query 1 in stream 0 using template query84.tpl
