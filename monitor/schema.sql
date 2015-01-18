--
-- 1. Cleanup database
-- 
DROP SCHEMA IF EXISTS moni CASCADE;
CREATE SCHEMA moni;

--
-- 2. Definition of fact tables
--

--CREATE TABLE moni.test_result
--(
--	query_name					VARCHAR(64),
--	start_time				    TIMESTAMP WITH TIME ZONE,
--	end_time					TIMESTAMP WITH TIME ZONE,
--);
-- 2.1 qd_info
CREATE TABLE moni.qd_info
(
	tr_id							INT,
	con_id							INT,
	query_start_time				TIMESTAMP WITH TIME ZONE,
	query_end_time					TIMESTAMP WITH TIME ZONE,
	user_name						VARCHAR(128),
	db_name							VARCHAR(128),
	query 							VARCHAR(10240)
)
DISTRIBUTED RANDOMLY;

-- 2.2 qd_mem_cpu
CREATE TABLE moni.qd_mem_cpu
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
	rss								DECIMAL(8,2),
	pmem							DECIMAL(4,1),
	pcpu							DECIMAL(4,1)
)
DISTRIBUTED RANDOMLY;

-- 2.3 qe_mem_cpu
CREATE TABLE moni.qe_mem_cpu
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
	rss								DECIMAL(8,2),
	pmem							DECIMAL(4,1),
	pcpu							DECIMAL(4,1)
)
DISTRIBUTED RANDOMLY;

--CREATE TABLE moni.run_info
--(
--	run_id							INT,
--	cluster_name					VARCHAR(128),
--	case_name						VARCHAR(128),
--	start_time						TIMESTAMP WITH TIME ZONE,
--	end_time						TIMESTAMP WITH TIME ZONE
--)
--DISTRIBUTED RANDOMLY;

--COPY moni.qe_mem_cpu FROM 'FNAME' WITH DELIMITER '|';