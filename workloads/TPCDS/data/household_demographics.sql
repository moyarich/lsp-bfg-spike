DROP TABLE IF EXISTS household_demographics_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_household_demographics_TABLESUFFIX;

create table household_demographics_TABLESUFFIX
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
) WITH (SQLSUFFIX) DISTRIBUTED BY(hd_demo_sk);

CREATE EXTERNAL TABLE e_household_demographics_TABLESUFFIX
(
hd_demo_sk                integer               ,
hd_income_band_sk         integer                       ,
hd_buy_potential          char(15)                      ,
hd_dep_count              integer                       ,
hd_vehicle_count          integer
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO household_demographics_TABLESUFFIX SELECT * FROM e_household_demographics_TABLESUFFIX;