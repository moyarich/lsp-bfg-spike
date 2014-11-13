DROP TABLE IF EXISTS item_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_item_TABLESUFFIX;

create table item_TABLESUFFIX
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      
) WITH (SQLSUFFIX) DISTRIBUTED BY(i_item_sk);

CREATE EXTERNAL TABLE e_item_TABLESUFFIX
(
i_item_sk                 integer               ,
i_item_id                 char(16)              ,
i_rec_start_date          date                          ,
i_rec_end_date            date                          ,
i_item_desc               varchar(200)                  ,
i_current_price           decimal(7,2)                  ,
i_wholesale_cost          decimal(7,2)                  ,
i_brand_id                integer                       ,
i_brand                   char(50)                      ,
i_class_id                integer                       ,
i_class                   char(50)                      ,
i_category_id             integer                       ,
i_category                char(50)                      ,
i_manufact_id             integer                       ,
i_manufact                char(50)                      ,
i_size                    char(20)                      ,
i_formulation             char(20)                      ,
i_color                   char(20)                      ,
i_units                   char(10)                      ,
i_container               char(10)                      ,
i_manager_id              integer                       ,
i_product_name            char(50)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');


INSERT INTO item_TABLESUFFIX SELECT * FROM e_item_TABLESUFFIX;