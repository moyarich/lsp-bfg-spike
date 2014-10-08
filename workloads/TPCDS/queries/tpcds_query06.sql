--@author guz4
--@description TPC-DS tpcds_query6
--@created 2013-03-06 18:02:02
--@created 2013-03-06 18:02:02
--@tags tpcds orca

-- start query 1 in stream 0 using template query6.tpl
select  a.ca_state state, count(*) cnt
 from customer_address_TABLESUFFIX a
     ,customer_TABLESUFFIX c
     ,store_sales_TABLESUFFIX s
     ,date_dim_TABLESUFFIX d
     ,item_TABLESUFFIX i
 where       a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
 	and d.d_month_seq = 
 	     (select distinct (d_month_seq)
 	      from date_dim
               where d_year = 2001
 	        and d_moy = 1 )
 	and i.i_current_price > 1.2 * 
             (select avg(j.i_current_price) 
 	     from item j 
 	     where j.i_category = i.i_category)
 group by a.ca_state
 having count(*) >= 10
 order by cnt 
 limit 100;

-- end query 1 in stream 0 using template query6.tpl
