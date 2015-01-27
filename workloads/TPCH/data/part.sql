DROP TABLE IF EXISTS part_TABLESUFFIX;
DROP EXTERNAL WEB TABLE IF EXISTS e_part_TABLESUFFIX;

CREATE TABLE part_TABLESUFFIX (
    P_PARTKEY     INTEGER NOT NULL,
    P_NAME        VARCHAR(55) NOT NULL,
    P_MFGR        CHAR(25) NOT NULL,
    P_BRAND       CHAR(10) NOT NULL,
    P_TYPE        VARCHAR(25) NOT NULL,
    P_SIZE        INTEGER NOT NULL,
    P_CONTAINER   CHAR(10) NOT NULL,
    P_RETAILPRICE DECIMAL(15,2) NOT NULL,
    P_COMMENT     VARCHAR(23) NOT NULL )
WITH (SQLSUFFIX)
DISTRIBUTED BY(P_PARTKEY);

CREATE EXTERNAL WEB TABLE e_part_TABLESUFFIX (
    P_PARTKEY     INTEGER,
    P_NAME        VARCHAR(55),
    P_MFGR        CHAR(25),
    P_BRAND       CHAR(10),
    P_TYPE        VARCHAR(25),
    P_SIZE        INTEGER,
    P_CONTAINER   CHAR(10),
    P_RETAILPRICE DECIMAL(15,2),
    P_COMMENT     VARCHAR(23) ) 
EXECUTE E'bash -c "$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T P -s SCALEFACTOR -N 64 -n $((GP_SEGMENT_ID + 1))"'
ON 64 FORMAT 'TEXT' (DELIMITER'|');

INSERT INTO part_TABLESUFFIX SELECT * FROM e_part_TABLESUFFIX;
