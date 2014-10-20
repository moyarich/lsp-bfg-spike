-- prep_exports.sql

-- This script create writable external tables for the loading demonstration
-- and the data export demonstration if the customer wishes to see it.

DROP EXTERNAL TABLE IF EXISTS retail_demo.order_lineitems_load;

CREATE EXTERNAL TABLE retail_demo.order_lineitems_load
(LIKE retail_demo.order_lineitems)
LOCATION (
'gpfdist://sdw1:8081/order_lineitems_load.dat',
'gpfdist://sdw1:8082/order_lineitems_load.dat',
'gpfdist://sdw2:8081/order_lineitems_load.dat',
'gpfdist://sdw2:8082/order_lineitems_load.dat',
'gpfdist://sdw3:8081/order_lineitems_load.dat',
'gpfdist://sdw3:8082/order_lineitems_load.dat',
'gpfdist://sdw4:8081/order_lineitems_load.dat',
'gpfdist://sdw4:8082/order_lineitems_load.dat'
)
FORMAT 'TEXT' ( DELIMITER '|' NULL '')
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.order_lineitems_export;

CREATE WRITABLE EXTERNAL TABLE retail_demo.order_lineitems_export
(LIKE retail_demo.order_lineitems)
LOCATION (
'gpfdist://sdw1:8081/order_lineitems_export.dat',
'gpfdist://sdw1:8082/order_lineitems_export.dat',
'gpfdist://sdw2:8081/order_lineitems_export.dat',
'gpfdist://sdw2:8082/order_lineitems_export.dat',
'gpfdist://sdw3:8081/order_lineitems_export.dat',
'gpfdist://sdw3:8082/order_lineitems_export.dat',
'gpfdist://sdw4:8081/order_lineitems_export.dat',
'gpfdist://sdw4:8082/order_lineitems_export.dat'
)
FORMAT 'TEXT' ( DELIMITER '|' NULL '')
DISTRIBUTED RANDOMLY
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.orders_load;

CREATE EXTERNAL TABLE retail_demo.orders_load
(LIKE retail_demo.orders)
LOCATION (
'gpfdist://sdw1:8081/orders_load.dat',
'gpfdist://sdw1:8082/orders_load.dat',
'gpfdist://sdw2:8081/orders_load.dat',
'gpfdist://sdw2:8082/orders_load.dat',
'gpfdist://sdw3:8081/orders_load.dat',
'gpfdist://sdw3:8082/orders_load.dat',
'gpfdist://sdw4:8081/orders_load.dat',
'gpfdist://sdw4:8082/orders_load.dat'
)
FORMAT 'TEXT' ( DELIMITER '|' NULL '')
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.orders_export;

CREATE WRITABLE EXTERNAL TABLE retail_demo.orders_export
(LIKE retail_demo.orders)
LOCATION (
'gpfdist://sdw1:8081/orders_export.dat',
'gpfdist://sdw1:8082/orders_export.dat',
'gpfdist://sdw2:8081/orders_export.dat',
'gpfdist://sdw2:8082/orders_export.dat',
'gpfdist://sdw3:8081/orders_export.dat',
'gpfdist://sdw3:8082/orders_export.dat',
'gpfdist://sdw4:8081/orders_export.dat',
'gpfdist://sdw4:8082/orders_export.dat'
)
FORMAT 'TEXT' ( DELIMITER '|' NULL '')
DISTRIBUTED RANDOMLY
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.shipment_lineitems_load;

CREATE EXTERNAL TABLE retail_demo.shipment_lineitems_load
(LIKE retail_demo.shipment_lineitems)
LOCATION (
'gpfdist://sdw1:8081/shipment_lineitems_load.dat',
'gpfdist://sdw1:8082/shipment_lineitems_load.dat',
'gpfdist://sdw2:8081/shipment_lineitems_load.dat',
'gpfdist://sdw2:8082/shipment_lineitems_load.dat',
'gpfdist://sdw3:8081/shipment_lineitems_load.dat',
'gpfdist://sdw3:8082/shipment_lineitems_load.dat',
'gpfdist://sdw4:8081/shipment_lineitems_load.dat',
'gpfdist://sdw4:8082/shipment_lineitems_load.dat'
)
FORMAT 'TEXT' ( DELIMITER '|' NULL '')
;

DROP EXTERNAL TABLE IF EXISTS retail_demo.shipment_lineitems_export;

CREATE WRITABLE EXTERNAL TABLE retail_demo.shipment_lineitems_export
(LIKE retail_demo.shipment_lineitems)
LOCATION (
'gpfdist://sdw1:8081/shipment_lineitems_export.dat',
'gpfdist://sdw1:8082/shipment_lineitems_export.dat',
'gpfdist://sdw2:8081/shipment_lineitems_export.dat',
'gpfdist://sdw2:8082/shipment_lineitems_export.dat',
'gpfdist://sdw3:8081/shipment_lineitems_export.dat',
'gpfdist://sdw3:8082/shipment_lineitems_export.dat',
'gpfdist://sdw4:8081/shipment_lineitems_export.dat',
'gpfdist://sdw4:8082/shipment_lineitems_export.dat'
)
FORMAT 'TEXT' ( DELIMITER '|' NULL '')
DISTRIBUTED RANDOMLY
;

