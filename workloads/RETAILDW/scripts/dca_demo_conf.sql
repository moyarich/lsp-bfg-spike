-- dca_demo_conf.sql
 
\set SCHEMA retail_demo
set enable_nestloop=off;

-- The number of rows you want to generate in the orders base table
-- 8.5 billion order rows translates to 10.4TB in the completed data set.
\set ORDERROWS 8500000000


-- The number of rows you want to reference in the customer dim table
-- The current data has a max of 54,996,000 customers
\set CUSTOMERROWS 54996000


-- The number of rows you want to reference in the product dim table
-- The current data has a max of 2,714,024 customers
\set PRODUCTROWS 2714024


-- Min and Max number of items per order
\set IC_MIN 1
\set IC_MAX 25


-- Dates for fact generation.  Data wil be generated and loaded into
-- the fact tables between the DATA_START and PRELOAD_END dates.
-- Data will be generated and loaded into files on the "ETL" nodes (sdw1-4)
-- between the PRELOAD_END and DATA_END dates.  The data files are used in 
-- the data loading portion of the demo.  It is suggested the PRELOAD_START
-- is January 1 (any year) and the DATA_END is a December 31 (any year).
-- The difference between PRELOAD_END and DATA_END defaults to roughly 2%
-- of the data.
\set DATA_START '\'2006-01-01\''
\set MONTHLY_END '\'2010-01-01\''
\set PRELOAD_END '\'2010-12-09\''
\set DATA_END '\'2010-12-31\''

