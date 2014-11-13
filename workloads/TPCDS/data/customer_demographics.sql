DROP TABLE IF EXISTS customer_demographics_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_customer_demographics_TABLESUFFIX;

create table customer_demographics_TABLESUFFIX
(
    cd_demo_sk                integer               not null,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       
) WITH (SQLSUFFIX) DISTRIBUTED BY(cd_demo_sk);


CREATE EXTERNAL TABLE e_customer_demographics_TABLESUFFIX
(
cd_demo_sk                integer               ,
cd_gender                 char(1)                       ,
cd_marital_status         char(1)                       ,
cd_education_status       char(20)                      ,
cd_purchase_estimate      integer                       ,
cd_credit_rating          char(10)                      ,
cd_dep_count              integer                       ,
cd_dep_employed_count     integer                       ,
cd_dep_college_count      integer
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO customer_demographics_TABLESUFFIX SELECT * FROM e_customer_demographics_TABLESUFFIX;