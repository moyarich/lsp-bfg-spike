-- prep_external_tables.sql

-- This script creates the external tables needed for dimension creation

\timing on

DROP EXTERNAL TABLE IF EXISTS retail_demo.websites_xt;
CREATE EXTERNAL TABLE retail_demo.websites_xt (
  Website_ID    INTEGER,
  Website_Name  VARCHAR(100)
)
LOCATION ('gpfdist://HOST:PORT/websites.dat')
FORMAT 'CSV'
SEGMENT REJECT LIMIT 10 ROWS
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.zip_city_state_xt;
CREATE EXTERNAL TABLE retail_demo.zip_city_state_xt (
  Zip_Code   VARCHAR(5),
  City       VARCHAR(50),
  State      CHAR(2)
)
LOCATION ('gpfdist://HOST:PORT/zip_codes.dat')
FORMAT 'CSV'
SEGMENT REJECT LIMIT 10 ROWS
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.male_first_names_xt;
CREATE EXTERNAL TABLE retail_demo.male_first_names_xt (
  First_name  VARCHAR(200)
)
LOCATION ('gpfdist://HOST:PORT/male_first_names.txt')
FORMAT 'CSV'
SEGMENT REJECT LIMIT 10 ROWS
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.female_first_names_xt;
CREATE EXTERNAL TABLE retail_demo.female_first_names_xt (
  First_name  VARCHAR(200)
)
LOCATION ('gpfdist://HOST:PORT/female_first_names.txt')
FORMAT 'CSV'
SEGMENT REJECT LIMIT 10 ROWS
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.surnames_xt;
CREATE EXTERNAL TABLE retail_demo.surnames_xt (
  surname  VARCHAR(200)
)
LOCATION ('gpfdist://HOST:PORT/surnames.dat')
FORMAT 'CSV'
SEGMENT REJECT LIMIT 10 ROWS
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.products_xt CASCADE;
CREATE EXTERNAL TABLE retail_demo.products_xt (
  Category_Name  VARCHAR(200),
  Product_Name   VARCHAR(2000),
  Product_Price  VARCHAR(20)
)
LOCATION ('gpfdist://HOST:PORT/products_full.dat')
FORMAT 'TEXT' (DELIMITER E'\t' ESCAPE E'\\')
SEGMENT REJECT LIMIT 10 ROWS
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.streets_xt;
CREATE EXTERNAL TABLE retail_demo.streets_xt (
  Street_name  VARCHAR(200)
)
LOCATION ('gpfdist://HOST:PORT/street_names.dat')
FORMAT 'TEXT' (DELIMITER '|' ESCAPE E'\\')
SEGMENT REJECT LIMIT 10 ROWS
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.state_sales_taxes_xt;
CREATE EXTERNAL TABLE retail_demo.state_sales_taxes_xt (
  State_Code  VARCHAR(2),
  Tax_Rate    NUMERIC(5,3)
)
LOCATION ('gpfdist://HOST:PORT/state_sales_tax.dat')
FORMAT 'CSV'
SEGMENT REJECT LIMIT 10 ROWS
;

\timing off
