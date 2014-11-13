DROP TABLE IF EXISTS store_returns_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_store_returns_TABLESUFFIX;

create table store_returns_TABLESUFFIX
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          bigint               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  
) WITH (SQLSUFFIX) DISTRIBUTED BY(sr_item_sk,sr_ticket_number)
PARTITION BY range(sr_returned_date_sk)
(
partition p1 start(2450815) INCLUSIVE end(2451179) INCLUSIVE, 
partition p2 start(2451180) INCLUSIVE end(2451544) INCLUSIVE, 
partition p3 start(2451545) INCLUSIVE end(2451910) INCLUSIVE, 
partition p4 start(2451911) INCLUSIVE end(2452275) INCLUSIVE, 
partition p5 start(2452276) INCLUSIVE end(2452640) INCLUSIVE, 
partition p6 start(2452641) INCLUSIVE end(2453005) INCLUSIVE
);

CREATE EXTERNAL TABLE e_store_returns_TABLESUFFIX
(
sr_returned_date_sk       integer                       ,
sr_return_time_sk         integer                       ,
sr_item_sk                integer               ,
sr_customer_sk            integer                       ,
sr_cdemo_sk               integer                       ,
sr_hdemo_sk               integer                       ,
sr_addr_sk                integer                       ,
sr_store_sk               integer                       ,
sr_reason_sk              integer                       ,
sr_ticket_number          bigint               ,
sr_return_quantity        integer                       ,
sr_return_amt             decimal(7,2)                  ,
sr_return_tax             decimal(7,2)                  ,
sr_return_amt_inc_tax     decimal(7,2)                  ,
sr_fee                    decimal(7,2)                  ,
sr_return_ship_cost       decimal(7,2)                  ,
sr_refunded_cash          decimal(7,2)                  ,
sr_reversed_charge        decimal(7,2)                  ,
sr_store_credit           decimal(7,2)                  ,
sr_net_loss               decimal(7,2)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO store_returns_TABLESUFFIX SELECT * FROM e_store_returns_TABLESUFFIX;