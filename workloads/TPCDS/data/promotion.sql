DROP TABLE IF EXISTS promotion_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_promotion_TABLESUFFIX;

create table promotion_TABLESUFFIX
(
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       
) WITH (SQLSUFFIX) DISTRIBUTED BY(p_promo_sk);

CREATE EXTERNAL TABLE e_promotion_TABLESUFFIX
(
p_promo_sk                integer               ,
p_promo_id                char(16)              ,
p_start_date_sk           integer                       ,
p_end_date_sk             integer                       ,
p_item_sk                 integer                       ,
p_cost                    decimal(15,2)                 ,
p_response_target         integer                       ,
p_promo_name              char(50)                      ,
p_channel_dmail           char(1)                       ,
p_channel_email           char(1)                       ,
p_channel_catalog         char(1)                       ,
p_channel_tv              char(1)                       ,
p_channel_radio           char(1)                       ,
p_channel_press           char(1)                       ,
p_channel_event           char(1)                       ,
p_channel_demo            char(1)                       ,
p_channel_details         varchar(100)                  ,
p_purpose                 char(15)                      ,
p_discount_active         char(1)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO promotion_TABLESUFFIX SELECT * FROM e_promotion_TABLESUFFIX;