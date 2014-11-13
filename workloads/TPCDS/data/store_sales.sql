DROP TABLE IF EXISTS store_sales_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_store_sales_TABLESUFFIX;

create table store_sales_TABLESUFFIX
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          bigint               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  
) WITH (SQLSUFFIX) DISTRIBUTED BY(ss_item_sk,ss_ticket_number)
PARTITION BY range(ss_sold_date_sk)
(
partition p1 start(2450815) INCLUSIVE end(2451179) INCLUSIVE, 
partition p2 start(2451180) INCLUSIVE end(2451544) INCLUSIVE, 
partition p3 start(2451545) INCLUSIVE end(2451910) INCLUSIVE, 
partition p4 start(2451911) INCLUSIVE end(2452275) INCLUSIVE, 
partition p5 start(2452276) INCLUSIVE end(2452640) INCLUSIVE, 
partition p6 start(2452641) INCLUSIVE end(2453005) INCLUSIVE
);

CREATE EXTERNAL TABLE e_store_sales_TABLESUFFIX
(
ss_sold_date_sk           integer                       ,
ss_sold_time_sk           integer                       ,
ss_item_sk                integer               ,
ss_customer_sk            integer                       ,
ss_cdemo_sk               integer                       ,
ss_hdemo_sk               integer                       ,
ss_addr_sk                integer                       ,
ss_store_sk               integer                       ,
ss_promo_sk               integer                       ,
ss_ticket_number          bigint               ,
ss_quantity               integer                       ,
ss_wholesale_cost         decimal(7,2)                  ,
ss_list_price             decimal(7,2)                  ,
ss_sales_price            decimal(7,2)                  ,
ss_ext_discount_amt       decimal(7,2)                  ,
ss_ext_sales_price        decimal(7,2)                  ,
ss_ext_wholesale_cost     decimal(7,2)                  ,
ss_ext_list_price         decimal(7,2)                  ,
ss_ext_tax                decimal(7,2)                  ,
ss_coupon_amt             decimal(7,2)                  ,
ss_net_paid               decimal(7,2)                  ,
ss_net_paid_inc_tax       decimal(7,2)                  ,
ss_net_profit             decimal(7,2)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO store_sales_TABLESUFFIX SELECT * FROM e_store_sales_TABLESUFFIX;