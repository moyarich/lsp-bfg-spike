DROP TABLE IF EXISTS customer_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_customer_TABLESUFFIX;

create table customer_TABLESUFFIX
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      
) WITH (SQLSUFFIX) DISTRIBUTED BY(c_customer_sk);

CREATE EXTERNAL TABLE e_customer_TABLESUFFIX
(
c_customer_sk             integer               ,
c_customer_id             char(16)              ,
c_current_cdemo_sk        integer                       ,
c_current_hdemo_sk        integer                       ,
c_current_addr_sk         integer                       ,
c_first_shipto_date_sk    integer                       ,
c_first_sales_date_sk     integer                       ,
c_salutation              char(10)                      ,
c_first_name              char(20)                      ,
c_last_name               char(30)                      ,
c_preferred_cust_flag     char(1)                       ,
c_birth_day               integer                       ,
c_birth_month             integer                       ,
c_birth_year              integer                       ,
c_birth_country           varchar(20)                   ,
c_login                   char(13)                      ,
c_email_address           char(50)                      ,
c_last_review_date        char(10)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO customer_TABLESUFFIX SELECT * FROM e_customer_TABLESUFFIX;