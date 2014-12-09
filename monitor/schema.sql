--
-- 1. Cleanup database
-- 
DROP SCHEMA IF EXISTS moni CASCADE;
CREATE SCHEMA moni;

--
-- 2. Definition of fact tables
--

-- 2.1 qd_info
CREATE TABLE moni.qd_info
(
	con_id							INT,
	query_start_time				TIMESTAMP WITH TIME ZONE,
	pid								INT,
	user_name						VARCHAR(128),
	db_name							VARCHAR(256)
) DISTRIBUTED RANDOMLY;

-- 2.2 qd_mem_cpu
CREATE TABLE moni.qd_mem_cpu
(
	time_point						TIMESTAMP WITH TIME ZONE,
	con_id							INT,
	rss								INT,
	pmem							DECIMAL(3,2),
	pcpu							DECIMAL(3,2)
) DISTRIBUTED RANDOMLY;

-- 2.3 qe_mem_cpu
CREATE TABLE moni.qe_mem_cpu
(
	time_point						TIMESTAMP WITH TIME ZONE,
	con_id							INT,
	seg_id							INT,
	status							VARCHAR(64),
	rss								INT,
	pmem							DECIMAL(3,2),
	pcpu							DECIMAL(3,2)
) DISTRIBUTED RANDOMLY;