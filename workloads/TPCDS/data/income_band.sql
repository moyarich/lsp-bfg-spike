DROP TABLE IF EXISTS income_band_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_income_band_TABLESUFFIX;

create table income_band_TABLESUFFIX
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
) WITH (SQLSUFFIX) DISTRIBUTED BY(ib_income_band_sk);

CREATE EXTERNAL TABLE e_income_band_TABLESUFFIX
(
ib_income_band_sk         integer               ,
ib_lower_bound            integer                       ,
ib_upper_bound            integer
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO income_band_TABLESUFFIX SELECT * FROM e_income_band_TABLESUFFIX;