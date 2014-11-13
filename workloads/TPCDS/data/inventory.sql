DROP TABLE IF EXISTS inventory_TABLESUFFIX CASCADE;
DROP EXTERNAL TABLE IF EXISTS e_inventory_TABLESUFFIX;

create table inventory_TABLESUFFIX
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       
) WITH (SQLSUFFIX) DISTRIBUTED BY(inv_date_sk,inv_item_sk,inv_warehouse_sk)
PARTITION BY range(inv_date_sk)
(
partition p1 start(2450815) INCLUSIVE end(2451179) INCLUSIVE, 
partition p2 start(2451180) INCLUSIVE end(2451544) INCLUSIVE, 
partition p3 start(2451545) INCLUSIVE end(2451910) INCLUSIVE, 
partition p4 start(2451911) INCLUSIVE end(2452275) INCLUSIVE, 
partition p5 start(2452276) INCLUSIVE end(2452640) INCLUSIVE
);

CREATE EXTERNAL TABLE e_inventory_TABLESUFFIX
(
inv_date_sk               integer               ,
inv_item_sk               integer               ,
inv_warehouse_sk          integer               ,
inv_quantity_on_hand      integer
)
LOCATION
FORMAT 'TEXT' (DELIMITER '|' NULL AS '');

INSERT INTO inventory_TABLESUFFIX SELECT * FROM e_inventory_TABLESUFFIX;