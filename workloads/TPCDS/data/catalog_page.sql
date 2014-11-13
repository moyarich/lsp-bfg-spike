DROP TABLE IF EXISTS catalog_page_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_catalog_page_TABLESUFFIX;

create table catalog_page_TABLESUFFIX
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
) WITH (SQLSUFFIX) DISTRIBUTED BY(cp_catalog_page_sk);

CREATE EXTERNAL TABLE e_catalog_page_TABLESUFFIX
(
cp_catalog_page_sk        integer               ,
cp_catalog_page_id        char(16)              ,
cp_start_date_sk          integer                       ,
cp_end_date_sk            integer                       ,
cp_department             varchar(50)                   ,
cp_catalog_number         integer                       ,
cp_catalog_page_number    integer                       ,
cp_description            varchar(100)                  ,
cp_type                   varchar(100)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO catalog_page_TABLESUFFIX SELECT * FROM e_catalog_page_TABLESUFFIX;