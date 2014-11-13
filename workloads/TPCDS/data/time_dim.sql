DROP TABLE IF EXISTS time_dim_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_time_dim_TABLESUFFIX;

create table time_dim_TABLESUFFIX
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
) WITH (SQLSUFFIX) DISTRIBUTED BY(t_time_sk);

CREATE EXTERNAL TABLE e_time_dim_TABLESUFFIX
(
t_time_sk                 integer               ,
t_time_id                 char(16)              ,
t_time                    integer                       ,
t_hour                    integer                       ,
t_minute                  integer                       ,
t_second                  integer                       ,
t_am_pm                   char(2)                       ,
t_shift                   char(20)                      ,
t_sub_shift               char(20)                      ,
t_meal_time               char(20)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO time_dim_TABLESUFFIX SELECT * FROM e_time_dim_TABLESUFFIX;