DROP TABLE IF EXISTS customer_address_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_customer_address_TABLESUFFIX;

create table customer_address_TABLESUFFIX
(
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                      
) WITH (SQLSUFFIX) DISTRIBUTED BY(ca_address_sk);


CREATE EXTERNAL TABLE e_customer_address_TABLESUFFIX
(
ca_address_sk             integer               ,
ca_address_id             char(16)              ,
ca_street_number          char(10)                      ,
ca_street_name            varchar(60)                   ,
ca_street_type            char(15)                      ,
ca_suite_number           char(10)                      ,
ca_city                   varchar(60)                   ,
ca_county                 varchar(30)                   ,
ca_state                  char(2)                       ,
ca_zip                    char(10)                      ,
ca_country                varchar(20)                   ,
ca_gmt_offset             decimal(5,2)                  ,
ca_location_type          char(20)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO customer_address_TABLESUFFIX SELECT * FROM e_customer_address_TABLESUFFIX;