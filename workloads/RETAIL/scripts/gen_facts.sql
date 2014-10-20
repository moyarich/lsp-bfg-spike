-- gen_facts.sql

-- This script uses the order_item_base table and the pre-populated dimension tables to "denormalize" and generate the fact tables.

-- 7 hours

\timing on

-- Load the "global" variables
PATH_OF_DCA_DEMO_CONF_SQL

--DROP INDEX IF EXISTS order_lineitems_cust_id;

TRUNCATE TABLE retail_demo.Order_LineItems;

INSERT INTO retail_demo.Order_LineItems (
  Order_ID
, Order_Item_ID
, Product_ID
, Product_Name
, Customer_ID
, Store_ID
, Item_Shipment_Status_Code
, Order_Datetime
, Ship_Datetime
, Item_Return_Datetime
, Item_Refund_Datetime
, Product_Category_ID
, Product_Category_Name
, Payment_Method_Code
, Tax_Amount
, Item_Quantity
, Item_Price
, Discount_Amount
, Coupon_Code
, Coupon_Amount
, Ship_Address_Line1
, Ship_Address_Line2
, Ship_Address_Line3
, Ship_Address_City
, Ship_Address_State
, Ship_Address_Postal_Code
, Ship_Address_Country
, Ship_Phone_Number
, Ship_Customer_Name
, Ship_Customer_Email_Address
, Ordering_Session_ID
, Website_URL
)
SELECT Order_ID
,      Order_Item_ID
,      ols.Product_ID
,      prod.Product_Name
,      ols.Customer_ID
,      Store_ID
,      CASE WHEN Item_Shipment_Status_Code < 3 THEN 'Received'
            WHEN Item_Shipment_Status_Code = 3 THEN 'Prep'
            WHEN Item_Shipment_Status_Code = 4 THEN 'Shipper_Notified'
       ELSE 'Shipped' END AS Item_Shipment_Status_Code
,      Order_TS AS Order_Datetime
,      ship_ts AS Ship_Datetime
,      item_return_ts AS Item_Return_Datetime
,      item_refund_ts AS Item_Refund_Datetime
,      prod.Category_ID
,      cat.Category_Name
,      CASE ols.Payment_Method_ID
            WHEN 1 THEN 'COD'
            WHEN 2 THEN 'Credit'
            WHEN 3 THEN 'CreditCard'
            WHEN 4 THEN 'GiftCertificate'
            WHEN 5 THEN 'FreeReplacement'
       END
       AS Payment_Method_Code
,      CASE WHEN Item_Quantity > 12 THEN Item_Quantity-11 ELSE 1 END * prod.Price * sst.Tax_Rate/100 AS Tax_Amount
,      CASE WHEN Item_Quantity > 12 THEN Item_Quantity-11 ELSE 1 END Item_Quantity
,      prod.Price AS Item_Price
,      CASE Discount_Amount_Code
            WHEN 1 THEN .05
            WHEN 2 THEN 0
            ELSE .05 + .015 * Discount_Amount_Code
       END AS Discount_Amount
,      CASE Coupon_Code
            WHEN 1 THEN 'BOGO-PDX'
            WHEN 2 THEN 'None'
            WHEN 3 THEN 'HOLIDAY'
            WHEN 4 THEN 'Coupon-'||TRANSLATE(bigrand,'0123456789','AKRNCPWLQVE')
            ELSE 'CUST-Ret-'||TRANSLATE(ols.customer_id,'0123456789','AKRNCPWLQVE')
       END AS Coupon_Code
,      CASE Coupon_Code
            WHEN 1 THEN .5
            WHEN 2 THEN 0
            WHEN 3 THEN .15
            WHEN 4 THEN .1 * Item_Quantity
            ELSE .05
       END AS Coupon_Amount
,      addr.house_number ||' '|| addr.street_name AS Ship_Address_Line1
,      addr.appt_suite_no AS Ship_Address_Line2
,      CASE WHEN SUBSTRING(bigrand for 2) >= 76 THEN 'ATTN: '||cust.first_name||' '||cust.last_name ELSE NULL END AS Ship_Address_Line3
,      addr.city AS Ship_Address_City
,      addr.state_code AS Ship_Address_State
,      addr.zip_code AS Ship_Address_Postal_Code
,      addr.country AS Ship_Address_Country
,      addr.phone_number AS Ship_Phone_Number
,      cust.first_name||' '||cust.last_name AS Ship_Customer_Name
,      eaddr.Email_Address
,      ols.Ordering_Session_ID
,      'http://myretailsite.emc.com/product_detail/'||
       TRANSLATE(cat.category_name,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ','abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_')||
       '/?tps='||TRANSLATE(bigrand::VARCHAR,'0123456789','ikvurmsqlps')||
       '&sessionid='||ols.Ordering_Session_ID||
       '&ref='||TRANSLATE(ols.customer_id::VARCHAR,'0123456789','zmeposjxya')||
       '&sku='||ols.product_id AS Website_URL
FROM   order_item_base ols
,      retail_demo.products_dim prod
,      retail_demo.categories_dim cat
,      retail_demo.customers_dim cust
,      retail_demo.Customer_Addresses_Dim addr
,      retail_demo.email_addresses_dim eaddr
,      retail_demo.state_sales_taxes sst
WHERE  ols.Product_ID = prod.Product_ID
AND    prod.Category_ID = cat.Category_ID
AND    ols.customer_id = cust.customer_id
AND    cust.customer_id = addr.customer_id
AND    cust.customer_id = eaddr.customer_id
AND    addr.state_code = sst.state_code
-- This should be the join but the data generation doesn't take slowly changing dimensions into account yet.  So, for now, we'll just use the recent address
--AND    addr.valid_from_timestamp <= Order_Date + (Order_Datetime_Offset||' seconds')::INTERVAL
--AND    COALESCE(addr.valid_from_timestamp, current_timestamp) >= Order_Date + (Order_Datetime_Offset||' seconds')::INTERVAL
AND    addr.valid_to_timestamp IS NULL
AND    ols.Order_TS < :PRELOAD_END -- Reserve ~2% for the example loads
;

VACUUM ANALYZE retail_demo.Order_LineItems;

TRUNCATE TABLE retail_demo.Orders;

set statement_mem='125MB';

INSERT INTO retail_demo.Orders (
  Order_ID                      
, Customer_ID                  
, Store_ID                     
, Order_Datetime               
, Ship_Completion_Datetime     
, Return_Datetime              
, Refund_Datetime              
, Payment_Method_Code          
, Total_Tax_Amount             
, Total_Paid_Amount             
, Total_Item_Quantity          
, Total_Discount_Amount        
, Coupon_Code                  
, Coupon_Amount                
, Order_Canceled_Flag          
, Has_Returned_Items_Flag       
, Has_Refunded_Items_Flag      
, Fraud_Code                   
, Fraud_Resolution_Code        
, Billing_Address_Line1        
, Billing_Address_Line2        
, Billing_Address_Line3        
, Billing_Address_City         
, Billing_Address_State        
, Billing_Address_Postal_Code  
, Billing_Address_Country      
, Billing_Phone_Number         
, Customer_Name                
, Customer_Email_Address       
, Ordering_Session_ID          
, Website_URL                  
)
SELECT Order_ID AS Order_ID
, MAX(Customer_ID) AS Customer_ID
, MAX(Store_ID) AS Store_ID
, MIN(Order_Datetime) AS Order_Datetime
, MAX(Ship_Datetime) AS Ship_Completion_Datetime
, MIN(item_Return_Datetime) AS Return_Datetime
, MIN(item_Refund_Datetime) AS Refund_Datetime
, MAX(Payment_Method_Code) AS Payment_Method_Code
, SUM(Tax_Amount) AS Total_Tax_Amount
, SUM(Item_Quantity * Item_Price) AS Total_Paid_Amount
, SUM(Item_Quantity) AS Total_Item_Quantity
, SUM(Discount_Amount) AS Total_Discount_Amount
, MAX(Coupon_Code) AS Coupon_Code
, MAX(Coupon_Amount) AS Coupon_Amount
, CASE WHEN SUM(CASE WHEN item_Return_Datetime IS NOT NULL OR item_Refund_Datetime IS NOT NULL THEN 1 ELSE 0 END) = COUNT(*) THEN 'Y' ELSE 'N' END AS Order_Canceled_Flag
, CASE WHEN MAX(item_Return_Datetime) IS NOT NULL THEN 'Y' ELSE 'N' END AS Has_Returned_Items_Flag
, CASE WHEN MAX(item_Refund_Datetime) IS NOT NULL THEN 'Y' ELSE 'N' END AS Has_Refunded_Items_Flag
, CASE SUBSTRING(order_id FOR 2 FROM 2)
       WHEN 96 THEN 'Stolen Card'
       WHEN 97 THEN 'Compromised Account'
       WHEN 98 THEN 'Gift Certificate Abuse'
       WHEN 99 THEN 'Researching'
       ELSE NULL
  END AS Fraud_Code
, CASE WHEN SUBSTRING(order_id FOR 2 FROM 2) > 95
       THEN CASE SUBSTRING(order_id FOR 1 FROM 6)
                 WHEN 5 THEN 'Resolved - Account Canceled'
                 WHEN 6 THEN 'Resolved - No Fraud'
                 WHEN 7 THEN 'Resolved - Order Canceled'
                 WHEN 8 THEN 'Resolved - Authorities Notified'
                 ELSE 'In Progress'
            END
       ELSE NULL
  END AS Fraud_Resolution_Code
, MAX(ship_Address_Line1) AS Billing_Address_Line1 -- Need to change this to a window function for first ship address as the billing address
, MAX(ship_Address_Line2) AS Billing_Address_Line2
, MAX(ship_Address_Line3) AS Billing_Address_Line3
, MAX(ship_Address_City) AS Billing_Address_City
, MAX(ship_Address_State) AS Billing_Address_State
, MAX(ship_Address_Postal_Code) AS Billing_Address_Postal_Code
, MAX(ship_Address_Country) AS Billing_Address_Country
, MAX(ship_Phone_Number) AS Billing_Phone_Number
, MAX(ship_Customer_Name) AS Customer_Name
, MAX(ship_Customer_Email_Address) AS Customer_Email_Address
, MAX(Ordering_Session_ID) AS Ordering_Session_ID
, MAX(Website_URL) AS Website_URL
FROM retail_demo.order_lineitems
GROUP BY order_id
;

set statement_mem='1999MB';

VACUUM ANALYZE retail_demo.Orders;

TRUNCATE TABLE retail_demo.Shipment_LineItems;

INSERT INTO retail_demo.Shipment_LineItems (
  Shipment_ID
, Order_ID
, Order_Item_ID
, Product_ID
, Product_Name
, Customer_ID
, Order_Datetime
, Ship_Datetime
, Item_Ship_Quantity
, Item_Price
, Customer_Paid_Ship_Cost
, Our_Paid_Ship_Cost
, Shipper_Code
, Shipment_Type_Code
, Ship_Address_Line1
, Ship_Address_Line2
, Ship_Address_Line3
, Ship_Address_City
, Ship_Address_State
, Ship_Address_Postal_Code
, Ship_Address_Country
, Ship_Phone_Number
, Ship_Customer_Name
, Ship_Customer_Email_Address
)
SELECT TO_CHAR(round(order_item_id/100000), 'FM00000') ||
       '-' || TO_CHAR(order_item_id%100000, 'FM00000') ||
       '-' || TO_CHAR(product_id%100, 'FM00') ||
       '-' || TO_CHAR(round(product_id*retail_demo.box_muller(3::bigint,37::bigint)/100)::int % 1000000 , 'FM000000')
       AS Shipment_ID
, Order_ID
, Order_Item_ID
, Product_ID
, Product_Name
, Customer_ID
, Order_Datetime
, Ship_Datetime
, Item_Quantity AS Item_Ship_Quantity
, Item_Price
, .8 * (5 + (.03*Item_Price)) AS Customer_Paid_Ship_Cost -- Ship cost is $5 + 3% of the item_cost.  Customer pays 80%.
, .2 * (5 + (.03*Item_Price)) AS Our_Paid_Ship_Cost      -- Eventually change customer/us paid ratio to "random"
, CASE retail_demo.crand(1::bigint,4::bigint)
       WHEN 1 THEN 'UPS'
       WHEN 2 THEN 'USPS'
       WHEN 3 THEN 'FedEx'
       WHEN 4 THEN 'DHL'
  END AS Shipper_Code
, CASE retail_demo.crand(1::bigint,4::bigint)
       WHEN 1 THEN 'Next Day Air'
       WHEN 2 THEN '2 Day Ground'
       WHEN 3 THEN '3 to 5 Day Ground'
       WHEN 4 THEN 'International'
  END AS Shipment_Type_Code
, Ship_Address_Line1
, Ship_Address_Line2
, Ship_Address_Line3
, Ship_Address_City
, Ship_Address_State
, Ship_Address_Postal_Code
, Ship_Address_Country
, Ship_Phone_Number
, Ship_Customer_Name
, Ship_Customer_Email_Address
FROM retail_demo.Order_LineItems
WHERE ship_datetime IS NOT NULL
;

VACUUM ANALYZE retail_demo.Shipment_LineItems;

-- The following is only needed if the demo will include web_user (BI style) queries.
--CREATE INDEX order_lineitems_cust_id ON retail_demo.order_lineitems (customer_id);

\timing off
