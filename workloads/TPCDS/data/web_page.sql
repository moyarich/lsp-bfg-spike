DROP TABLE IF EXISTS web_page_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_web_page_TABLESUFFIX;

create table web_page_TABLESUFFIX
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                      
) WITH (SQLSUFFIX) DISTRIBUTED BY(wp_web_page_sk);

CREATE EXTERNAL TABLE e_web_page_TABLESUFFIX
(
wp_web_page_sk            integer               ,
wp_web_page_id            char(16)              ,
wp_rec_start_date         date                          ,
wp_rec_end_date           date                          ,
wp_creation_date_sk       integer                       ,
wp_access_date_sk         integer                       ,
wp_autogen_flag           char(1)                       ,
wp_customer_sk            integer                       ,
wp_url                    varchar(100)                  ,
wp_type                   char(50)                      ,
wp_char_count             integer                       ,
wp_link_count             integer                       ,
wp_image_count            integer                       ,
wp_max_ad_count           integer
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO web_page_TABLESUFFIX SELECT * FROM e_web_page_TABLESUFFIX;