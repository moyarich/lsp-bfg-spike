DROP TABLE IF EXISTS ship_mode_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_ship_mode_TABLESUFFIX;

create table ship_mode_TABLESUFFIX
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
) WITH (SQLSUFFIX) DISTRIBUTED BY(sm_ship_mode_sk);

CREATE EXTERNAL TABLE e_ship_mode_TABLESUFFIX
(
sm_ship_mode_sk           integer               ,
sm_ship_mode_id           char(16)              ,
sm_type                   char(30)                      ,
sm_code                   char(10)                      ,
sm_carrier                char(20)                      ,
sm_contract               char(20)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO ship_mode_TABLESUFFIX SELECT * FROM e_ship_mode_TABLESUFFIX;