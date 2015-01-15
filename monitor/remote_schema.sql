--INSERT INTO hst.qd_info_history select * from hst.qd_info;
DROP TABLE IF EXISTS hst.qd_info;
CREATE TABLE hst.qd_info
(
	tr_id							INT,
	con_id							INT,
	query_start_time				TIMESTAMP WITH TIME ZONE,
	query_end_time					TIMESTAMP WITH TIME ZONE,
--	pid								INT,
	user_name						VARCHAR(128),
	db_name							VARCHAR(128),
	query 							VARCHAR(10240)
);

-- 2.2 qd_mem_cpu
--INSERT INTO hst.qd_mem_cpu_history select * from hst.qd_mem_cpu;
DROP TABLE IF EXISTS hst.qd_mem_cpu;
CREATE TABLE hst.qd_mem_cpu
(
	tr_id							INT,
	hostname						VARCHAR(32),
	timeslot						INT,
	real_time						TIMESTAMP WITH TIME ZONE,
	pid								INT,
	ppid							INT,
	con_id							INT,
	cmd								VARCHAR(16),
	status							VARCHAR(16),
	rss								DECIMAL(10,2),
	pmem							DECIMAL(4,1),
	pcpu							DECIMAL(4,1)
);

-- 2.3 qe_mem_cpu
--INSERT INTO hst.qe_mem_cpu_history select * from hst.qe_mem_cpu;
DROP TABLE IF EXISTS hst.qe_mem_cpu;
CREATE TABLE hst.qe_mem_cpu
(
	tr_id							INT,
	hostname						VARCHAR(32),
	timeslot						INT,
	real_time						TIMESTAMP WITH TIME ZONE,
	pid								INT,
	con_id							INT,
	seg_id							VARCHAR(16),
	cmd								VARCHAR(16),
	slice							VARCHAR(16),
	status							VARCHAR(16),
	rss								DECIMAL(10,2),
	pmem							DECIMAL(4,1),
	pcpu							DECIMAL(4,1)
);


--COPY hst.qe_mem_cpu FROM 'FNAME' WITH DELIMITER '|';