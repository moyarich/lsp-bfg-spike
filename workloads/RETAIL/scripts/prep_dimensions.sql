-- prep_dimensions.sql

-- This script will create, load, and analyze all dimension tables.

\timing on

DROP TABLE IF EXISTS retail_demo.state_sales_taxes CASCADE;

CREATE TABLE retail_demo.state_sales_taxes AS SELECT * FROM retail_demo.state_sales_taxes_xt;

DROP TABLE IF EXISTS retail_demo.products_dim CASCADE;

CREATE TABLE retail_demo.products_dim (
  Product_ID      SERIAL          NOT NULL,
  Category_ID     INTEGER         NOT NULL,
  Price           DECIMAL(15,2)   NOT NULL,
  Product_Name    VARCHAR(2000)   NOT NULL
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED BY (Product_ID)
;

ALTER SEQUENCE retail_demo.products_dim_product_id_seq CACHE 1000;

DROP TABLE IF EXISTS retail_demo.categories_dim CASCADE;

CREATE TABLE retail_demo.categories_dim (
  Category_ID    SERIAL         NOT NULL,
  Category_Name  VARCHAR(400)   NOT NULL
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED RANDOMLY
;

DROP TABLE IF EXISTS retail_demo.email_addresses_dim CASCADE;

CREATE TABLE retail_demo.email_addresses_dim (
  Customer_ID     INTEGER        NOT NULL,
  Email_Address   VARCHAR(500)   NOT NULL
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED BY (Customer_ID)
;

DROP TABLE IF EXISTS retail_demo.Date_Dim CASCADE;

CREATE TABLE retail_demo.Date_Dim AS (
SELECT calendar_day
,      EXTRACT(year FROM calendar_day)::SMALLINT AS reporting_year
,      EXTRACT(quarter FROM calendar_day)::SMALLINT AS reporting_quarter
,      EXTRACT(month FROM calendar_day)::SMALLINT AS reporting_month
,      EXTRACT(week FROM calendar_day)::SMALLINT AS reporting_week
,      EXTRACT(dow FROM calendar_day)::SMALLINT AS reporting_dow
FROM  (SELECT current_date + generate_series('2005-01-01' - current_date, '2012-12-31' - current_date) as calendar_day) AS foo
)
DISTRIBUTED BY (calendar_day)
;

DROP TABLE IF EXISTS retail_demo.Customers_Dim CASCADE;

CREATE TABLE retail_demo.Customers_Dim (
  Customer_ID    SERIAL         NOT NULL,
  First_Name     VARCHAR(100)   NOT NULL,
  Last_Name      VARCHAR(200)   NOT NULL,
  Gender         CHAR(1)
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED BY (Customer_ID)
;

ALTER SEQUENCE retail_demo.customers_dim_customer_id_seq CACHE 1000;

DROP TABLE IF EXISTS retail_demo.payment_methods CASCADE;

CREATE TABLE retail_demo.payment_methods (
  Payment_Method_ID    SMALLINT,
  Payment_Method_Code  VARCHAR(20)
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED RANDOMLY
;

INSERT INTO retail_demo.payment_methods VALUES (1,'COD');
INSERT INTO retail_demo.payment_methods VALUES (2,'Credit');
INSERT INTO retail_demo.payment_methods VALUES (3,'CreditCard');
INSERT INTO retail_demo.payment_methods VALUES (4,'GiftCertificate');
INSERT INTO retail_demo.payment_methods VALUES (5,'FreeReplacement');


DROP TABLE IF EXISTS retail_demo.Customer_Addresses_Dim CASCADE;

CREATE TABLE retail_demo.Customer_Addresses_Dim (
  Customer_Address_ID  SERIAL         NOT NULL,
  Customer_ID          INTEGER        NOT NULL,
  Valid_From_Timestamp TIMESTAMP      NOT NULL DEFAULT current_timestamp,
  Valid_To_Timestamp   TIMESTAMP,
  House_Number         VARCHAR(20),
  Street_Name          VARCHAR(150),
  Appt_Suite_No        VARCHAR(50),
  City                 VARCHAR(200),
  State_Code           VARCHAR(2),
  Zip_Code             VARCHAR(5),
  Zip_Plus_Four        VARCHAR(10),
  Country              VARCHAR(10),
  Phone_Number         VARCHAR(20)
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED BY (Customer_ID)
;

ALTER SEQUENCE retail_demo.customer_addresses_dim_customer_address_id_seq CACHE 1000;

DROP TABLE IF EXISTS retail_demo.city_state_zip CASCADE;

CREATE TABLE retail_demo.city_state_zip (
  csz_id     SERIAL,
  Zip_Code   VARCHAR(5),
  City       VARCHAR(50),
  State      CHAR(2)
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED randomly
;


DROP TABLE IF EXISTS retail_demo.streets CASCADE;

CREATE TABLE retail_demo.streets (
  street_ID    SERIAL,
  Street_name  VARCHAR(200)
)
WITH (appendonly=true, compresstype=quicklz)
DISTRIBUTED randomly
;

INSERT INTO retail_demo.categories_dim (category_name)
SELECT DISTINCT category_name FROM retail_demo.products_xt;


INSERT INTO retail_demo.products_dim (category_id, price, product_name)
SELECT cat.category_id
,      TRANSLATE(prod.product_price,'0123456789.'||prod.product_price,'0123456789.')::NUMERIC
,      prod.product_name
FROM   retail_demo.products_xt prod
,      retail_demo.categories_dim cat
WHERE  prod.category_name = cat.category_name ;


INSERT INTO retail_demo.customers_dim (first_name, last_name, gender)
SELECT first_name, ln.surname, gender
FROM  (SELECT first_name, 'M' AS gender FROM retail_demo.male_first_names_xt
       UNION ALL
       SELECT first_name, 'F' AS gender FROM retail_demo.female_first_names_xt
      )first_names
,     (SELECT surname
       FROM   retail_demo.surnames_xt
      )ln 
;

INSERT INTO retail_demo.city_state_zip (zip_Code, City, State)
SELECT zip_code, city, state
FROM   retail_demo.zip_city_state_xt 
;

INSERT INTO retail_demo.streets (street_name)
SELECT street_name
FROM   retail_demo.streets_xt 
;


INSERT INTO retail_demo.email_addresses_dim (Customer_ID, Email_Address)
SELECT cust.Customer_ID
,      cust.custname||'@'||w.Website_Name AS Email_Address
FROM   (SELECT Customer_ID
        ,      First_Name ||'.'||Last_Name AS custname
        ,      retail_demo.crand(1::bigint,50000::bigint)::integer AS websites_key
        FROM   retail_demo.Customers_Dim
       )cust
,       retail_demo.websites_xt w
WHERE   cust.websites_key = w.Website_ID
;

-- The Customer_Addresses_Dim table is a type 2 slowly changing dimension.
-- Customers have between 1 and 15 addresses with the most recent being "active"
-- Most customers have one address.


INSERT INTO retail_demo.Customer_Addresses_Dim (
  Customer_ID
, Valid_From_Timestamp
, Valid_To_Timestamp
, House_Number
, Street_Name
, Appt_Suite_No
, City
, State_Code
, Zip_Code
, Zip_Plus_Four
, Country
, Phone_Number)
SELECT Customer_ID
,      Valid_From_Timestamp
,      LAG(Valid_From_Timestamp,1) OVER (PARTITION BY customer_id ORDER BY VALID_From_Timestamp DESC) AS Valid_To_Timestamp
,      House_Number
,      Street_Name
,      Appt_Suite_No
,      City
,      State
,      Zip_Code
,      Zip_Plus_Four
,      Country
,      '('||TO_CHAR(retail_demo.crand(1::bigint,999::bigint),'FM000') ||')'|| TO_CHAR(retail_demo.crand(1::bigint,999::bigint),'FM000') ||'-'|| TO_CHAR(retail_demo.crand(1::bigint,9999::bigint),'FM0000') AS Phone_Number
FROM (
SELECT Customer_ID
,      current_date - (Initial_Address_Days_Ago||' Days')::INTERVAL + (Initial_Address_Sec_Offset||' Seconds')::INTERVAL AS Valid_From_Timestamp
,      NULL AS Valid_To_Timestamp
,      House_Number
,      s.Street_Name
,      CASE WHEN apt_val <= 75 THEN NULL ELSE 'Apt '||apt_val-75 END AS Appt_Suite_No
,      csz.City
,      csz.State
,      csz.Zip_Code
,      csz.Zip_Code ||'-'||retail_demo.crand(1::bigint,9999::bigint) AS Zip_Plus_Four
,      'USA' AS Country
FROM  (SELECT Customer_ID
       ,      multiplier
       ,      address_count
       ,      retail_demo.crand(1::bigint,9999::bigint) AS House_Number
       ,      retail_demo.crand(1::bigint,4225::bigint)::integer AS street_key -- We have 4,225 rows in the streets table
       ,      retail_demo.crand(1::bigint,99::bigint) AS Apt_Val
       ,      retail_demo.crand(1::bigint,29470::bigint)::integer AS zip_key -- We have 28,470 zip codes in the city_state_zip table
       ,      retail_demo.crand(1::bigint,365*5::bigint) AS Initial_Address_Days_Ago
       ,      retail_demo.crand(1::bigint,60*60*24::bigint) AS Initial_Address_Sec_Offset
       FROM  (SELECT Customer_ID
              ,      retail_demo.power_rand(1::bigint,15::bigint,5::int)::integer AS address_count
              FROM   retail_demo.customers_dim
             )cust
       ,     (SELECT generate_series(1,15)::integer AS multiplier) foobar
       WHERE  multiplier <= cust.address_count
      ) AS cust
,      retail_demo.city_state_zip csz
,      retail_demo.streets s
WHERE  csz.csz_id = cust.zip_key
AND    s.street_id = cust.street_key
) AS addrs
;

-- Set statistics target to a higher level than the default for important columns
ALTER TABLE retail_demo.email_addresses_dim ALTER COLUMN customer_id set statistics 1000;
ALTER TABLE retail_demo.Customer_Addresses_Dim ALTER COLUMN customer_id set statistics 1000;
ALTER TABLE retail_demo.Customer_Addresses_Dim ALTER COLUMN valid_to_timestamp set statistics 1000;
ALTER TABLE retail_demo.customers_dim ALTER COLUMN customer_id set statistics 1000;

VACUUM ANALYZE retail_demo.state_sales_taxes;
VACUUM ANALYZE retail_demo.products_dim;
VACUUM ANALYZE retail_demo.categories_dim;
VACUUM ANALYZE retail_demo.email_addresses_dim;
VACUUM ANALYZE retail_demo.Date_Dim;
VACUUM ANALYZE retail_demo.Customers_Dim;
VACUUM ANALYZE retail_demo.payment_methods;
VACUUM ANALYZE retail_demo.Customer_Addresses_Dim;
VACUUM ANALYZE retail_demo.city_state_zip;
VACUUM ANALYZE retail_demo.streets;

GRANT SELECT ON retail_demo.products_dim TO public;
GRANT SELECT ON retail_demo.categories_dim TO public;
GRANT SELECT ON retail_demo.email_addresses_dim TO public;
GRANT SELECT ON retail_demo.Date_Dim TO public;
GRANT SELECT ON retail_demo.Customers_Dim TO public;
GRANT SELECT ON retail_demo.Customer_Addresses_Dim TO public;

\timing off
