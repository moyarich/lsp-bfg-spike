-- start_ignore
DROP TABLE IF EXISTS hawq_test;
-- end_ignore
CREATE TABLE hawq_test ( cid INT, cdate TIMESTAMP) DISTRIBUTED BY (cid);
INSERT INTO hawq_test SELECT generate_series(1, 100000), CURRENT_TIMESTAMP;
SELECT COUNT(*) FROM hawq_test;
DROP TABLE IF EXISTS hawq_test;
