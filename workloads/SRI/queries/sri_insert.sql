BEGIN;
INSERT INTO sri_table (tid, bdate, aid, delta, mtime) values (nextval('sri_seq'),'YYYY-MM-DD', 1, 1, current_timestamp);
COMMIT;
SELECT COUNT(*) FROM sri_table;

