-- gen_order_base.sql
--
-- This script generates two tables--an order base table and an order_item base table with the following attibutes:
-- * There is a daily variation of order volumes based on the day of the week (Mondays are bigger sales days than Fridays)
-- * There is a monthly variation of order volumes (December is bigger than February)
-- * As our fictional company is growing, each year's sales are larger than the previous year (this may be modified to show a shrinking business)
-- * Some customers buy a lot and some customers buy a little
-- * All customers have an inception date and some have a departure date
-- * Most orders have a single item but the count can be much higher (the number of orders with a higher item count decreases rapidly)
-- * Orders are occasionally returned or refunded

-- 2 hours

\timing on
 
-- Load the "Global Variables"
PATH_OF_DCA_DEMO_CONF_SQL


CREATE TEMPORARY TABLE date_weights ( 
   period VARCHAR
 , value INT
 , factor NUMERIC(7,3) ) 
DISTRIBUTED RANDOMLY;

INSERT INTO date_weights VALUES

-- DAY OF WEEK WEIGHTS: Mondays are typical peak days, for example
   ('DOW',0, 16.0)
 , ('DOW',1, 24.0)
 , ('DOW',2, 16.0)
 , ('DOW',3, 14.0)
 , ('DOW',4, 10.0)
 , ('DOW',5,  8.0)
 , ('DOW',6, 12.0)

-- MONTH OF YEAR WEIGHTS, Q4 tends to be the heavy 

 , ('MONTH', 1,   5.0)
 , ('MONTH', 2,   6.0) 
 , ('MONTH', 3,   7.0)
 , ('MONTH', 4,   7.0)
 , ('MONTH', 5,   7.0)
 , ('MONTH', 6,   7.0)
 , ('MONTH', 7,   7.0)
 , ('MONTH', 8,   8.0)
 , ('MONTH', 9,   9.0)
 , ('MONTH',10,  10.0)
 , ('MONTH',11,  13.0)
 , ('MONTH',12,  14.0)

-- YEAR OVER YEAR WEIGHTS

 , ('YEAR', 1990,  45)
 , ('YEAR', 1991,  46)
 , ('YEAR', 1992,  47)
 , ('YEAR', 1993,  48)
 , ('YEAR', 1994,  48)
 , ('YEAR', 1995,  52)
 , ('YEAR', 1996,  50)
 , ('YEAR', 1997,  51)
 , ('YEAR', 1998,  55)
 , ('YEAR', 1999,  57)
 , ('YEAR', 2000,  59)
 , ('YEAR', 2001,  61)
 , ('YEAR', 2002,  65)
 , ('YEAR', 2003,  69)
 , ('YEAR', 2004,  71)
 , ('YEAR', 2005,  91)
 , ('YEAR', 2006,  89)
 , ('YEAR', 2007,  91)
 , ('YEAR', 2008,  95)
 , ('YEAR', 2009,  95)
 , ('YEAR', 2010, 103)
 , ('YEAR', 2011, 111)
 , ('YEAR', 2012, 120)
 , ('YEAR', 2013, 121)
 , ('YEAR', 2014, 131)
 , ('YEAR', 2015, 143)
 , ('YEAR', 2016, 147)
 , ('YEAR', 2017, 152)
 , ('YEAR', 2018, 154)
 , ('YEAR', 2019, 166)
 , ('YEAR', 2020, 170)
;

-- , ('YEAR', 2006,   8.0)
-- , ('YEAR', 2007,  10.0)
-- , ('YEAR', 2008,  13.0)
-- , ('YEAR', 2009,  17.0)
-- , ('YEAR', 2010,  23.0)


-- The date base table includes a row for every day that we are creating along with a 
-- (somewhat) normalized factor value.  The weights below can be used to skew the relative
-- importance of one of the 4 date classifications. Keep in mind the weights above are not
-- normalized.

CREATE TEMPORARY TABLE DATE_BASE AS 
SELECT calendar_day
-- this will add +/- 4%ish of variability per day
,      (  SUM( factor ) *  
         ( 1.0 + ( retail_demo.box_muller(-4::bigint,4::bigint,3::smallint,false) / 100.0 )) 
       ) as factor
FROM (SELECT calendar_day
      ,      dw.period
      ,      dw.value
      ,      dw.factor
      FROM   retail_demo.date_dim dt
      ,      date_weights dw 
      WHERE  date_part( dw.period, calendar_day ) = dw.value
      AND    calendar_day BETWEEN :DATA_START AND :DATA_END
     ) foo
GROUP BY calendar_day
DISTRIBUTED BY ( calendar_day );

VACUUM ANALYZE DATE_BASE;

-- 1-row agg table holding the date based aggs for later use.
DROP  TABLE IF EXISTS date_aggs;
CREATE TABLE date_aggs AS
SELECT min( calendar_day ) as first_day
     , max( calendar_day ) as last_day
     , max( factor ) as max_factor
     , sum( factor ) as sum_factor
  FROM DATE_BASE
DISTRIBUTED RANDOMLY;

VACUUM ANALYZE date_aggs;

-- Create a table that contains the customer start and end dates (when
-- the customer first ordered and last ordered)
DROP TABLE IF EXISTS cust_dates;

CREATE TABLE cust_dates AS
SELECT customer_id
     , start_date
     , CASE WHEN Duration_days = 0 
            THEN last_day
            WHEN start_date + interval '1 day' * Duration_days >= last_day
            THEN last_day
            ELSE start_date + interval '1 day' * Duration_days
       END as last_date
     , retail_demo.box_muller(1::bigint,10::bigint)::smallint as customer_segment
  FROM (
SELECT C.customer_id 
     , first_day
     , last_day
     , CASE WHEN retail_demo.power_rand( 0, 10, 5 ) = 0 
            THEN first_day
            ELSE last_day - 
                 ( interval '1 day' * 
                   LEAST( retail_demo.box_muller_half( retail_demo.crand(1,10), (last_day - first_day)::bigint, 2::smallint, false )
                        , (last_day - first_day)::bigint
                        ) 
                 ) 
       END AS start_date 
     , retail_demo.power_rand( 0, 10, 5 )*retail_demo.box_muller(1::bigint,50::bigint,3::smallint,true) as Duration_days
  FROM retail_demo.Customers_Dim C
     , date_aggs da 
       ) x
DISTRIBUTED BY ( customer_id );

VACUUM ANALYZE cust_dates;

DROP TABLE IF EXISTS dly_custs;
CREATE TABLE dly_custs AS
SELECT calendar_day
     , customer_id
     , customer_segment
  FROM (
         SELECT calendar_day
              , customer_id
              , customer_segment
              , retail_demo.rand_flag( (:ORDERROWS * ( factor / a.sum_factor )) / cust_count ) as placed_order
          FROM (
                SELECT calendar_day
                     , customer_id
                     , customer_segment
                     , MAX( d.factor ) OVER ( partition by calendar_day ) as factor
                     , count(*) OVER ( partition by calendar_day ) as cust_count
                  FROM date_base d
                     , cust_dates c
                 WHERE d.calendar_day between c.start_date and c.last_date
               ) md1
             , date_aggs a
       ) md2
 WHERE placed_order
DISTRIBUTED BY ( calendar_day );


VACUUM ANALYZE dly_custs;

CREATE TEMPORARY SEQUENCE orderseq  START 10000000 CACHE 1000;
CREATE TEMPORARY SEQUENCE cdseq START 213456789 CACHE 1000;

DROP TABLE IF EXISTS ORDER_BASE;
CREATE TABLE ORDER_BASE
WITH (APPENDONLY=TRUE, compresstype=quicklz)
AS 
SELECT order_id
     , calendar_day
     , customer_id
     , order_ts
     , bigseq
     , bigrand 
     , store_id
     , payment_method_id
     , item_count
     , customer_segment
     , 'ON' || TO_CHAR(round(order_id / 100000), 'FM00000') || 
       '-'  || TO_CHAR(order_id % 100000, 'FM00000') || 
       '-'  || TO_CHAR(bigseq % 100, 'FM00') || 
       '-'  || TO_CHAR(bigrand % 1000000 , 'FM000000')
        AS Order_num
     , 'OS' || TO_CHAR(bigseq % 100000, 'FM00000') || 
       '-'  || TO_CHAR(round(bigrand*13/100)::bigint % 1000000, 'FM000000') || 
       '-'  || TO_CHAR(customer_segment, 'FM00') || 
       '-'  || TO_CHAR(customer_id % 100000, 'FM00000') 
       AS Ordering_Session_ID 
   FROM ( -- Select the customers that purchased on this day
          SELECT calendar_day
               , customer_id
               , nextval( 'orderseq' ) as order_id
               , nextval( 'cdseq' )::bigint as bigseq
               , retail_demo.crand(1::bigint,9999999999::bigint) as bigrand
               , calendar_day + retail_demo.crand(0::bigint,86399::bigint) * interval '1 sec' AS Order_TS
               , retail_demo.box_muller(1::bigint,100::bigint)::smallint AS Store_ID
               , retail_demo.crand(1::bigint,5::bigint)::smallint AS Payment_Method_ID
               , retail_demo.power_rand(:IC_MIN::bigint,:IC_MAX::bigint,16::int)::smallint as item_count
               , customer_segment
            FROM dly_custs 
        ) md2
DISTRIBUTED BY ( order_id );

VACUUM ANALYZE ORDER_BASE;

CREATE TEMPORARY TABLE ic
AS 
SELECT ic::SMALLINT
  FROM generate_series(:IC_MIN, :IC_MAX ) ic
DISTRIBUTED RANDOMLY;

VACUUM ANALYZE ic;

CREATE TEMPORARY SEQUENCE orderitemseq START 10000000 CACHE 1000;

DROP TABLE IF EXISTS ORDER_ITEM_BASE;

CREATE TABLE ORDER_ITEM_BASE
WITH (APPENDONLY=TRUE, compresstype=quicklz)
AS 
SELECT order_id 
     , calendar_day
     , Customer_ID
     , nextval( 'orderitemseq' ) as order_item_id
     , bigrand
     , bigseq
     , Order_TS
     , store_id
     , retail_demo.box_muller(1::bigint,10::bigint)::smallint AS Item_Shipment_Status_Code
     , ic::smallint as Item_Ctr
     , item_count as Item_Count
     , retail_demo.box_muller(1::bigint, :PRODUCTROWS::bigint)::INT AS Product_ID
     , calendar_day 
       + interval '1 second' 
       * retail_demo.box_muller(86400::bigint, 604800::bigint, 3::smallint, false ) -- Between 1 - 7 days
       AS Ship_TS
     , CASE WHEN retail_demo.rand_flag(0.9) 
            THEN NULL::timestamp
            ELSE calendar_day 
                 + interval '1 second' 
                 * retail_demo.box_muller(604800::bigint,2592000::bigint, 3::smallint, false) 
       END AS Item_Return_TS
     , CASE WHEN retail_demo.rand_flag( 0.966 ) 
            THEN NULL::timestamp
            ELSE calendar_day 
               + interval '1 second' 
               * retail_demo.box_muller(604800::bigint,2592000::bigint, 3::smallint, false) 
       END AS Item_Refund_TS
     , payment_Method_ID
     , retail_demo.power_rand(1::bigint,25::bigint,15::int)::smallint AS Item_Quantity 
     , retail_demo.power_rand(1::bigint,25::bigint,15::int)::smallint AS Discount_Amount_Code
     , retail_demo.box_muller_half(1::bigint,10::bigint)::smallint AS Coupon_Code 
     , order_num
     , ordering_session_id
  FROM ORDER_BASE o
     , ic
 WHERE o.item_count >= ic.ic
DISTRIBUTED BY (order_id);

VACUUM ANALYZE ORDER_ITEM_BASE;

\timing off
