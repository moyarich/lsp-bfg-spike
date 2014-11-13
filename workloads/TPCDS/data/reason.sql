DROP TABLE IF EXISTS reason_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_reason_TABLESUFFIX;

create table reason_TABLESUFFIX
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     
) WITH (SQLSUFFIX) DISTRIBUTED BY(r_reason_sk);

CREATE EXTERNAL TABLE e_reason_TABLESUFFIX
(
r_reason_sk               integer               ,
r_reason_id               char(16)              ,
r_reason_desc             char(100)
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO reason_TABLESUFFIX SELECT * FROM e_reason_TABLESUFFIX;