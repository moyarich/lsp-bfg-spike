DROP TABLE IF EXISTS warehouse_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS warehouse_ext;

create table warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  
) distributed by (w_warehouse_sk);

CREATE EXTERNAL TABLE warehouse_ext
(
w_warehouse_sk            integer               ,
w_warehouse_id            char(16)              ,
w_warehouse_name          varchar(20)                   ,
w_warehouse_sq_ft         integer                       ,
w_street_number           char(10)                      ,
w_street_name             varchar(60)                   ,
w_street_type             char(15)                      ,
w_suite_number            char(10)                      ,
w_city                    varchar(60)                   ,
w_county                  varchar(30)                   ,
w_state                   char(2)                       ,
w_zip                     char(10)                      ,
w_country                 varchar(20)                   ,
w_gmt_offset              decimal(5,2)
)
LOCATION_warehouse_ext
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO warehouse SELECT * FROM warehouse_ext;