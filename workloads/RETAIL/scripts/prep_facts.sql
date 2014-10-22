-- prep_facts.sql

-- This script creates the fact tables.
-- The default is 5 years of data with row-oriented, monthly partitions for 4
-- years and weekly, column-oriented partitions for the most recent year.
-- Changes to the date ranges in this script should be mirrored in the

\timing on 

PATH_OF_DCA_DEMO_CONF_SQL

DROP TABLE IF EXISTS retail_demo.Order_LineItems CASCADE;

CREATE TABLE retail_demo.Order_LineItems (
  Order_ID                      VARCHAR(21),
  Order_Item_ID                 BIGSERIAL
, Product_ID                    INTEGER
, Product_Name                  VARCHAR(2000)
, Customer_ID                   INTEGER
, Store_ID                      INTEGER
, Item_Shipment_Status_Code     VARCHAR(30)
, Order_Datetime                TIMESTAMP
, Ship_Datetime                 TIMESTAMP
, Item_Return_Datetime          TIMESTAMP
, Item_Refund_Datetime          TIMESTAMP
, Product_Category_ID           INTEGER
, Product_Category_Name         VARCHAR(200)
, Payment_Method_Code           VARCHAR(20)
, Tax_Amount                    DECIMAL(15,5)
, Item_Quantity                 INTEGER
, Item_Price                    DECIMAL(10,2)
, Discount_Amount               DECIMAL(15,5)
, Coupon_Code                   VARCHAR(20)
, Coupon_Amount                 DECIMAL(15,5)
, Ship_Address_Line1            VARCHAR(200)
, Ship_Address_Line2            VARCHAR(200)
, Ship_Address_Line3            VARCHAR(200)
, Ship_Address_City             VARCHAR(200)
, Ship_Address_State            VARCHAR(200)
, Ship_Address_Postal_Code      VARCHAR(20)
, Ship_Address_Country          VARCHAR(200)
, Ship_Phone_Number             VARCHAR(20)
, Ship_Customer_Name            VARCHAR(200)
, Ship_Customer_Email_Address   VARCHAR(200)
, Ordering_Session_ID           VARCHAR(30)
, Website_URL                   VARCHAR(500)
)
WITH (SQLSUFFIX)
DISTRIBUTED BY (Order_ID)
PARTITION BY RANGE (order_datetime) (
  START (:DATA_START) END (:MONTHLY_END) EVERY (interval '1 month') WITH (SQLSUFFIX),
  START (:MONTHLY_END) END (:PRELOAD_END) EVERY (interval '1 week') WITH (SQLSUFFIX),
  PARTITION today START (:PRELOAD_END) END (:DATA_END) INCLUSIVE WITH (SQLSUFFIX),
  DEFAULT PARTITION default_part
)
;

ALTER SEQUENCE retail_demo.order_lineitems_order_item_id_seq CACHE 100000;

DROP TABLE IF EXISTS retail_demo.Orders CASCADE;

CREATE TABLE retail_demo.Orders (
  Order_ID                      VARCHAR(21)
, Customer_ID                   INTEGER
, Store_ID                      INTEGER
, Order_Datetime                TIMESTAMP
, Ship_Completion_Datetime      TIMESTAMP
, Return_Datetime               TIMESTAMP
, Refund_Datetime               TIMESTAMP
, Payment_Method_Code           VARCHAR(20)
, Total_Tax_Amount              DECIMAL(15,5)
, Total_Paid_Amount             DECIMAL(15,5)
, Total_Item_Quantity           INTEGER
, Total_Discount_Amount         DECIMAL(15,5)
, Coupon_Code                   VARCHAR(20)
, Coupon_Amount                 DECIMAL(15,5)
, Order_Canceled_Flag           VARCHAR(1)
, Has_Returned_Items_Flag       VARCHAR(1)
, Has_Refunded_Items_Flag       VARCHAR(1)
, Fraud_Code                    VARCHAR(40)
, Fraud_Resolution_Code         VARCHAR(40)
, Billing_Address_Line1         VARCHAR(200)
, Billing_Address_Line2         VARCHAR(200)
, Billing_Address_Line3         VARCHAR(200)
, Billing_Address_City          VARCHAR(200)
, Billing_Address_State         VARCHAR(200)
, Billing_Address_Postal_Code   VARCHAR(20)
, Billing_Address_Country       VARCHAR(200)
, Billing_Phone_Number          VARCHAR(20)
, Customer_Name                 VARCHAR(200)
, Customer_Email_Address        VARCHAR(200)
, Ordering_Session_ID           VARCHAR(30)
, Website_URL                   VARCHAR(500)
)
WITH (SQLSUFFIX)
DISTRIBUTED BY (Order_ID)
PARTITION BY RANGE (order_datetime) (
  START (:DATA_START) END (:MONTHLY_END) EVERY (interval '1 month') WITH (SQLSUFFIX),
  START (:MONTHLY_END) END (:PRELOAD_END) EVERY (interval '1 week') WITH (SQLSUFFIX),
  PARTITION today START (:PRELOAD_END) END (:DATA_END) INCLUSIVE WITH (SQLSUFFIX),
  DEFAULT PARTITION default_part
)
;  


DROP TABLE IF EXISTS retail_demo.Shipment_LineItems CASCADE;

CREATE TABLE retail_demo.Shipment_LineItems (
  Shipment_ID                   VARCHAR(21)
, Shipment_Item_ID              BIGSERIAL
, Order_ID                      VARCHAR(21)
, Order_Item_ID                 BIGINT
, Product_ID                    INTEGER
, Product_Name                  VARCHAR(2000)
, Customer_ID                   INTEGER
, Order_Datetime                TIMESTAMP
, Ship_Datetime                 TIMESTAMP
, Item_Ship_Quantity            INTEGER
, Item_Price                    DECIMAL(10,2)
, Customer_Paid_Ship_Cost       DECIMAL(10,2)
, Our_Paid_Ship_Cost            DECIMAL(10,2)
, Shipper_Code                  VARCHAR(20)
, Shipment_Type_Code            VARCHAR(20)
, Ship_Address_Line1            VARCHAR(200)
, Ship_Address_Line2            VARCHAR(200)
, Ship_Address_Line3            VARCHAR(200)
, Ship_Address_City             VARCHAR(200)
, Ship_Address_State            VARCHAR(200)
, Ship_Address_Postal_Code      VARCHAR(20)
, Ship_Address_Country          VARCHAR(200)
, Ship_Phone_Number             VARCHAR(20)
, Ship_Customer_Name            VARCHAR(200)
, Ship_Customer_Email_Address   VARCHAR(200)
)
WITH (SQLSUFFIX)
DISTRIBUTED BY (Order_ID)
PARTITION BY RANGE (Ship_Datetime) (
  START (:DATA_START) END (:MONTHLY_END) EVERY (interval '1 month') WITH (SQLSUFFIX),
  START (:MONTHLY_END) END (:PRELOAD_END) EVERY (interval '1 week') WITH (SQLSUFFIX),
  PARTITION today START (:PRELOAD_END) END (:DATA_END) INCLUSIVE WITH (SQLSUFFIX),
  DEFAULT PARTITION default_part
)
;

ALTER SEQUENCE retail_demo.shipment_lineitems_shipment_item_id_seq CACHE 100000;

GRANT SELECT ON retail_demo.orders TO public;
GRANT SELECT ON retail_demo.order_lineitems TO public;
GRANT SELECT ON retail_demo.shipment_lineitems TO public;

\timing off
