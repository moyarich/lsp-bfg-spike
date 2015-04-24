--
-- 1. Cleanup database
--
DROP SCHEMA IF EXISTS hst CASCADE;
CREATE SCHEMA hst;



--
-- 2. Definition of fact tables
--


-- 2.1 HDFS setting
CREATE TABLE hst.hdfs_settings
(
    hds_id		SERIAL,
    hds_security	BOOLEAN DEFAULT TRUE,
    hds_ha		BOOLEAN DEFAULT TRUE,
    hds_description	VARCHAR(512)
);


-- 2.2 HAWQ settings
CREATE TABLE hst.hawq_settings
(
    hqs_id		SERIAL,
    hqs_num_segment	INT DEFAULT 4,
    hqs_kerberos	BOOLEAN DEFAULT TRUE,
    hqs_description	VARCHAR(512)
);


-- 2.3 Cluster settings
CREATE TABLE hst.cluster_settings
(
    cs_id		SERIAL,
    cs_name		VARCHAR(128),
    hds_id		INT,
    hqs_id		INT,
    cs_cpu		VARCHAR(128),
    cs_memory		VARCHAR(128),
    cs_disk		VARCHAR(128),
    cs_num_node	INT,
    cs_hdfs_roles	VARCHAR(512),
    cs_hawq_roles	VARCHAR(512),
    cs_description	VARCHAR(512)
);


-- 2.4 Workload
CREATE TABLE hst.workload
(
    wl_id			SERIAL,
    wl_name			VARCHAR(512),
    wl_catetory		VARCHAR(256),
    wl_data_volume_type	VARCHAR(16),
    wl_data_volume_size	INT,
    wl_appendonly		BOOLEAN DEFAULT TRUE,
    wl_orientation		VARCHAR(16),
    wl_row_group_size		INT DEFAULT NULL,
    wl_page_size		INT DEFAULT NULL,
    wl_compression_type	VARCHAR(16),
    wl_compression_level	SMALLINT,
    wl_partitions		INT DEFAULT 0,
    wl_iteration		INT DEFAULT 1,
    wl_concurrency		INT DEFAULT 1,
    wl_query_order		VARCHAR(16) DEFAULT 'SEQUENTIAL'
);


-- 2.5 Resource queue
CREATE TABLE hst.resource_queue
(
    rq_id		SERIAL,
    rq_name		VARCHAR(256),
    rq_definition	VARCHAR(256)
);


-- 2.6 Users
CREATE TABLE hst.users
(
    us_id	SERIAL,
    us_name	VARCHAR(256),
    rq_id	INT
);


-- 2.7 Scenario
CREATE TABLE hst.scenario
(
    s_id	SERIAL,
    cs_id	INT,
    wl_id	INT,
    us_id	INT
);


-- 2.8 Test run
CREATE TABLE hst.test_run
(
    tr_id		SERIAL,
    pulse_build_id	VARCHAR(256),
    pulse_build_url	VARCHAR(256),    
    hdfs_version	VARCHAR(256),
    hawq_version	VARCHAR(256),
    start_time		TIMESTAMP WITH TIME ZONE,
    end_time		TIMESTAMP WITH TIME ZONE,
    duration		INT,
    description	VARCHAR(512)
);


-- 2.9 Test result
CREATE TABLE hst.test_result
(
    tr_id		INT,
    s_id		INT,
    con_id      INT,
    action_type	VARCHAR(128),
    action_target	VARCHAR(128),
    iteration		INT,
    stream		INT,
    status		VARCHAR(16),
    start_time		TIMESTAMP WITH TIME ZONE,
    end_time		TIMESTAMP WITH TIME ZONE,
    duration		INT,
    output		VARCHAR(10240),
    plan		VARCHAR(10240),
    resource_usage	VARCHAR(10240),
    adj_s_id     int
);


-- 2.10 Test baseline
CREATE TABLE hst.test_baseline
(
    hdfs_version	VARCHAR(256),
    hawq_version	VARCHAR(256),
    s_id		INT,
    action_type	VARCHAR(128),
    action_target	VARCHAR(128),
    iteration		INT,
    stream		INT,
    status		VARCHAR(16),
    start_time		TIMESTAMP WITH TIME ZONE,
    end_time		TIMESTAMP WITH TIME ZONE,
    duration		INT,
    output		VARCHAR(10240),
    plan		VARCHAR(10240),
    resource_usage	VARCHAR(10240)
);



--
-- 3. Definition of reporting views
--
DROP FUNCTION IF EXISTS hst.f_precompute_test_baseline(baseline_hdfs_version TEXT, baseline_hawq_version TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_baseline(baseline_hdfs_version TEXT, baseline_hawq_version TEXT);
DROP FUNCTION IF EXISTS hst.f_precompute_test_result(test_run_id INT);
DROP FUNCTION IF EXISTS hst.f_generate_test_result(test_run_id INT);
DROP FUNCTION IF EXISTS hst.f_precompute_test_result(start_run_id INT,end_run_id INT);
DROP FUNCTION IF EXISTS hst.f_generate_test_result(start_run_id INT,end_run_id INT);


DROP FUNCTION IF EXISTS hst.f_generate_test_report_detail_internal(actual_test_result_table TEXT, baseline_test_result_table TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_detail(test_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_detail(test_run_id_actual INT, test_run_id_baseline INT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_detail(start_run_id INT, end_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_detail(start_run_id_actual INT,end_run_id_actual INT, start_run_id_baseline INT,end_run_id_baseline INT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_detail(baseline1_hdfs_version TEXT, baseline1_hawq_version TEXT, baseline2_hdfs_version TEXT, baseline2_hawq_version TEXT);

DROP FUNCTION IF EXISTS hst.f_generate_test_report_summary_internal(test_report_table TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_summary(test_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_summary(test_run_id_actual INT, test_run_id_baseline INT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_summary(baseline1_hdfs_version TEXT, baseline1_hawq_version TEXT, baseline2_hdfs_version TEXT, baseline2_hawq_version TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_summary(start_run_id INT, end_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT);
DROP FUNCTION IF EXISTS hst.f_generate_test_report_summary(start_run_id_actual INT,end_run_id_actual INT, start_run_id_baseline INT,end_run_id_baseline INT);


CREATE OR REPLACE FUNCTION hst.f_precompute_test_baseline(baseline_hdfs_version TEXT, baseline_hawq_version TEXT)
RETURNS TABLE(s_id INT, adj_s_id int, action_type VARCHAR(128), action_target VARCHAR(128), status TEXT, duration DECIMAL(18,2))
AS $$
BEGIN
    RETURN QUERY
    SELECT tb.s_id, tb.adj_s_id, tb.action_type, tb.action_target,
           CASE WHEN SUM(CASE WHEN tb.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
           ELSE 'PASS'::TEXT END status,
           AVG(tb.duration)::Decimal(18,2) AS duration
    FROM hst.test_baseline tb
    WHERE tb.hdfs_version = baseline_hdfs_version AND
          tb.hawq_version = baseline_hawq_version AND
          tb.s_id IN (SELECT scenario.s_id
                       FROM hst.scenario
                       WHERE (scenario.wl_id IN (SELECT workload.wl_id FROM hst.workload WHERE workload.wl_concurrency = 1)))
    GROUP BY tb.s_id, tb.adj_s_id ,tb.action_type, tb.action_target
    UNION  
    SELECT tb.s_id, tb.adj_s_id,'Execution' ::varchar(128) as action_type, 'CONCURRENT_QUERY'::varchar(128) as action_target,
           CASE WHEN SUM(CASE WHEN tb.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                      ELSE 'PASS'::TEXT END status,
           AVG(tb.duration) AS duration  
    FROM (
          SELECT tb.s_id, tb.adj_s_id,'Execution' as action_type, 'CONCURRENT_QUERY' as action_target,
                 CASE WHEN SUM(CASE WHEN tb.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                      ELSE 'PASS'::TEXT END status,
                 SUM(tb.duration) AS duration 
          FROM (
                 SELECT tb.s_id, tb.adj_s_id,tb.action_type, tb.stream, tb.action_target,
                 CASE WHEN SUM(CASE WHEN tb.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                      ELSE 'PASS'::TEXT END status,
                 AVG(tb.duration) AS duration
                 FROM hst.test_baseline tb
                 WHERE  tb.hdfs_version = baseline_hdfs_version AND
                        tb.hawq_version = baseline_hawq_version AND
                        tb.action_type = 'Execution' AND
                        tb.s_id IN (SELECT scenario.s_id
                                    FROM hst.scenario
                                    WHERE (scenario.wl_id IN (SELECT workload.wl_id FROM hst.workload WHERE workload.wl_concurrency > 1)))
                 GROUP BY tb.s_id, tb.adj_s_id,tb.action_type, tb.stream, tb.action_target) AS tb 
          GROUP BY tb.s_id, tb.adj_s_id,tb.stream) AS tb
    GROUP BY tb.s_id,tb.adj_s_id;
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_generate_test_baseline(baseline_hdfs_version TEXT, baseline_hawq_version TEXT)
RETURNS TABLE(s_id INT, adj_s_id INT, wl_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), status TEXT, duration DECIMAL(18,2), wl_catetory VARCHAR(256), wl_data_volume_type VARCHAR(16), wl_data_volume_size INT, wl_appendonly BOOLEAN, wl_disrandomly  BOOLEAN, wl_orientation VARCHAR(16), wl_row_group_size INT, wl_page_size INT, wl_compression_type VARCHAR(16), wl_compression_level SMALLINT, wl_partitions INT, wl_iteration INT, wl_concurrency INT, wl_query_order VARCHAR(16))
AS $$
BEGIN
    RETURN QUERY
    SELECT tb.s_id, tb. adj_s_id , w.wl_id, 
           CASE WHEN w.wl_concurrency > 1 THEN (w.wl_name || '_CONCURRENT')::VARCHAR(512)
                ELSE w.wl_name END wl_name, 
           tb.action_type, tb.action_target, tb.status, tb.duration,
           w.wl_catetory, w.wl_data_volume_type, w.wl_data_volume_size, w.wl_appendonly, w. wl_disrandomly, w.wl_orientation,
           w.wl_row_group_size, w.wl_page_size, w.wl_compression_type, w.wl_compression_level, w.wl_partitions,
           w.wl_iteration, w.wl_concurrency, w.wl_query_order
    FROM (SELECT * FROM hst.f_precompute_test_baseline(baseline_hdfs_version, baseline_hawq_version)) AS tb,
          hst.scenario AS s,
          hst.workload AS w
    WHERE tb.s_id = s.s_id AND s.wl_id = w.wl_id;
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_precompute_test_result(start_run_id INT, end_run_id INT)
RETURNS TABLE(tr_id INT, s_id INT, adj_s_id INT, action_type VARCHAR(128), action_target VARCHAR(128), status TEXT, duration DECIMAL(18,2))
AS $$
BEGIN

    RETURN QUERY
    SELECT tr.tr_id as tr_id, tr.s_id, tr.adj_s_id, tr.action_type, tr.action_target,
           CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
               WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
               ELSE 'PASS'::TEXT
           END AS status,
           CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT, 'SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 0::DECIMAL(18,2)
                ELSE AVG(tr.duration)::DECIMAL(18,2) 
           END AS duration
    FROM hst.test_result tr
    WHERE tr.s_id IN (SELECT scenario.s_id
                       FROM hst.scenario
                       WHERE (scenario.wl_id IN (SELECT workload.wl_id FROM hst.workload WHERE workload.wl_concurrency = 1)))
         AND tr.tr_id between start_run_id and end_run_id
    GROUP BY tr.tr_id,tr.s_id, tr.adj_s_id, tr.action_type, tr.action_target
    UNION  
    SELECT tr.tr_id, tr.s_id, tr.adj_s_id,'Execution' ::varchar(128) as action_type, 'CONCURRENT_QUERY'::varchar(128) as action_target,
           CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
                WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                ELSE 'PASS'::TEXT
           END AS status,
           AVG(tr.duration)::DECIMAL(18,2) AS duration
    FROM (
          SELECT tr.tr_id, tr.s_id, tr.adj_s_id,'Execution' as action_type, 'CONCURRENT_QUERY' as action_target,
                   CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
                        WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                        ELSE 'SUCCESS'::TEXT END AS status,
                   SUM(tr.duration) AS duration
          FROM (
               SELECT tr.tr_id as tr_id, tr.s_id, tr.adj_s_id, tr.stream,tr.action_type, tr.action_target,
                    CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
                        WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                        ELSE 'PASS'::TEXT
                    END AS status,
                    CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT, 'SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 0::DECIMAL(18,2)
                         ELSE AVG(tr.duration)::DECIMAL(18,2) 
                    END AS duration
              FROM hst.test_result tr
              WHERE tr.s_id IN (SELECT scenario.s_id FROM hst.scenario
                                WHERE (scenario.wl_id IN (SELECT workload.wl_id FROM hst.workload WHERE workload.wl_concurrency > 1)))
                   AND tr.tr_id between start_run_id and end_run_id AND tr.action_type = 'Execution'
             GROUP BY tr.tr_id,tr.s_id,tr.adj_s_id,  tr.action_type, tr.action_target, tr.stream) AS tr
          GROUP BY tr.tr_id, tr.s_id,tr.adj_s_id, tr.stream) AS tr
    GROUP BY tr.tr_id, tr.s_id,tr.adj_s_id;
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_precompute_test_result(test_run_id INT)
RETURNS TABLE(tr_id INT, s_id INT, adj_s_id INT, action_type VARCHAR(128), action_target VARCHAR(128), status TEXT, duration DECIMAL(18,2))
AS $$
BEGIN

    RETURN QUERY
    SELECT tr.tr_id as tr_id, tr.s_id, tr.adj_s_id, tr.action_type, tr.action_target,
           CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
               WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
               ELSE 'PASS'::TEXT
           END AS status,
           CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT, 'SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 0::DECIMAL(18,2)
                ELSE AVG(tr.duration)::DECIMAL(18,2) 
           END AS duration
    FROM hst.test_result tr
    WHERE tr.s_id IN (SELECT scenario.s_id
                       FROM hst.scenario
                       WHERE (scenario.wl_id IN (SELECT workload.wl_id FROM hst.workload WHERE workload.wl_concurrency = 1)))
         AND tr.tr_id = test_run_id
    GROUP BY tr.tr_id,tr.s_id, tr.adj_s_id, tr.action_type, tr.action_target
    UNION  
    SELECT tr.tr_id, tr.s_id, tr.adj_s_id,'Execution' ::varchar(128) as action_type, 'CONCURRENT_QUERY'::varchar(128) as action_target,
           CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
                WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                ELSE 'PASS'::TEXT
           END AS status,
           AVG(tr.duration)::DECIMAL(18,2) AS duration
    FROM (
          SELECT tr.tr_id, tr.s_id, tr.adj_s_id,'Execution' as action_type, 'CONCURRENT_QUERY' as action_target,
                   CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
                        WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                        ELSE 'SUCCESS'::TEXT END AS status,
                   SUM(tr.duration) AS duration
          FROM (
               SELECT tr.tr_id as tr_id, tr.s_id, tr.adj_s_id, tr.stream,tr.action_type, tr.action_target,
                    CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'ERROR'::TEXT
                        WHEN SUM(CASE WHEN tr.status::TEXT IN('SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 'SKIP'::TEXT
                        ELSE 'PASS'::TEXT
                    END AS status,
                    CASE WHEN SUM(CASE WHEN tr.status::TEXT IN('ERROR'::TEXT, 'SKIP'::TEXT) THEN 1 ELSE 0 END) > 0 THEN 0::DECIMAL(18,2)
                         ELSE AVG(tr.duration)::DECIMAL(18,2) 
                    END AS duration
              FROM hst.test_result tr
              WHERE tr.s_id IN (SELECT scenario.s_id FROM hst.scenario
                                WHERE (scenario.wl_id IN (SELECT workload.wl_id FROM hst.workload WHERE workload.wl_concurrency > 1)))
                   AND tr.tr_id = test_run_id AND tr.action_type = 'Execution'
             GROUP BY tr.tr_id,tr.s_id,tr.adj_s_id,  tr.action_type, tr.action_target, tr.stream) AS tr
          GROUP BY tr.tr_id, tr.s_id,tr.adj_s_id, tr.stream) AS tr
    GROUP BY tr.tr_id, tr.s_id,tr.adj_s_id;
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_generate_test_result(test_run_id INT)
RETURNS TABLE(tr_id INT, s_id INT,adj_s_id INT, wl_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), status TEXT, duration DECIMAL(18,2), wl_catetory VARCHAR(256), wl_data_volume_type VARCHAR(16), wl_data_volume_size INT, wl_appendonly BOOLEAN, wl_disrandomly  BOOLEAN, wl_orientation VARCHAR(16), wl_row_group_size INT, wl_page_size INT, wl_compression_type VARCHAR(16), wl_compression_level SMALLINT, wl_partitions INT, wl_iteration INT, wl_concurrency INT, wl_query_order VARCHAR(16))
AS $$
BEGIN
    RETURN QUERY
    SELECT tr.tr_id, tr.s_id, tr.adj_s_id,w.wl_id, 
           CASE WHEN w.wl_concurrency > 1 THEN (w.wl_name || '_CONCURRENT')::VARCHAR(512)
                ELSE w.wl_name END wl_name,
           tr.action_type, tr.action_target, tr.status, tr.duration,
           w.wl_catetory, w.wl_data_volume_type, w.wl_data_volume_size, w.wl_appendonly, w.wl_disrandomly,w.wl_orientation,
           w.wl_row_group_size, w.wl_page_size, w.wl_compression_type, w.wl_compression_level, w.wl_partitions,
           w.wl_iteration, w.wl_concurrency, w.wl_query_order
    FROM (SELECT * FROM hst.f_precompute_test_result(test_run_id)) AS tr,
          hst.scenario AS s,
          hst.workload AS w
    WHERE tr.s_id = s.s_id AND s.wl_id = w.wl_id;
END
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION hst.f_generate_test_result(start_run_id INT,end_run_id INT)
RETURNS TABLE(tr_id INT, s_id INT,adj_s_id INT, wl_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), status TEXT, duration DECIMAL(18,2), wl_catetory VARCHAR(256), wl_data_volume_type VARCHAR(16), wl_data_volume_size INT, wl_appendonly BOOLEAN, wl_disrandomly  BOOLEAN, wl_orientation VARCHAR(16), wl_row_group_size INT, wl_page_size INT, wl_compression_type VARCHAR(16), wl_compression_level SMALLINT, wl_partitions INT, wl_iteration INT, wl_concurrency INT, wl_query_order VARCHAR(16))
AS $$
BEGIN
    RETURN QUERY
    SELECT tr.tr_id, tr.s_id, tr.adj_s_id,w.wl_id, 
           CASE WHEN w.wl_concurrency > 1 THEN (w.wl_name || '_CONCURRENT')::VARCHAR(512)
                ELSE w.wl_name END wl_name,
           tr.action_type, tr.action_target, tr.status, tr.duration,
           w.wl_catetory, w.wl_data_volume_type, w.wl_data_volume_size, w.wl_appendonly, w.wl_disrandomly,w.wl_orientation,
           w.wl_row_group_size, w.wl_page_size, w.wl_compression_type, w.wl_compression_level, w.wl_partitions,
           w.wl_iteration, w.wl_concurrency, w.wl_query_order
    FROM (SELECT * FROM hst.f_precompute_test_result(start_run_id,end_run_id)) AS tr,
          hst.scenario AS s,
          hst.workload AS w
    WHERE tr.s_id = s.s_id AND s.wl_id = w.wl_id;
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_detail_internal(actual_test_result_table TEXT, baseline_test_result_table TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), actual_execution_time DECIMAL(18,2), baseline_execution_time DECIMAL(18,2), test_result TEXT, deviation DECIMAL(18,2), revised_baseline_execution_time DECIMAL(18,2), success_status INT, improve_status INT, failure_status INT, skip_status INT, error_status INT)
AS $$
BEGIN
    CREATE TEMP TABLE tmp_test_report_detail ON COMMIT DROP AS 
    SELECT ts.tr_id AS tr_id , ts.s_id, ts.wl_name, ts.action_type, ts.action_target, ts.duration as actual_execution_time, tb.duration as baseline_execution_time,
        CASE WHEN tb.status IS NULL THEN 'PASS AS NEW TEST CASE'
             WHEN ts.status IN ('SKIP', 'ERROR') OR tb.duration ::FLOAT <= 0 OR tb.status IN ('SKIP', 'ERROR') THEN ts.status
             WHEN (tb.duration >= 100 AND ts.duration::FLOAT / tb.duration::FLOAT BETWEEN 0.9 AND 1.1) OR (tb.duration < 100 AND ts.duration::FLOAT / tb.duration::FLOAT BETWEEN 0.3 AND 3)THEN 'PASS'
             WHEN (tb.duration >= 100 AND ts.duration::FLOAT / tb.duration::FLOAT < 0.9) OR (tb.duration <= 100 AND ts.duration::FLOAT / tb.duration::FLOAT < 0.3) THEN 'PASS WITH PERFORMANCE IMPROVEMENT'
             WHEN (tb.duration >= 100 AND ts.duration::FLOAT / tb.duration::FLOAT > 1.1) OR (tb.duration <= 100 AND ts.duration::FLOAT / tb.duration::FLOAT > 3) THEN 'FAILURE WITH PERFORMANCE DOWNGRADE'
             ELSE  'ERROR WITH TEST REPORT GENERATION'::TEXT
        END AS test_result,

        CASE WHEN tb.status IS NULL OR tb.duration = 0 OR ts.status IN ('SKIP', 'ERROR') OR ts.duration IS NULL THEN NULL
             ELSE (ts.duration::FLOAT / tb.duration::FLOAT)::DECIMAL(18,2)
        END AS deviation
        FROM actual_test_result_table AS ts
    LEFT JOIN baseline_test_result_table AS tb
    ON tb.s_id = ts.s_id AND ts.action_type = tb.action_type AND ts.action_target = tb.action_target
    UNION
    SELECT ts.tr_id AS tr_id , ts.s_id, ts.wl_name || '_RWITHD' as wl_name, ts.action_type, ts.action_target, ts.duration as actual_execution_time, tb.duration as baseline_execution_time,
        CASE WHEN tb.status IS NULL THEN 'PASS AS NEW TEST CASE'
             WHEN ts.status IN ('SKIP', 'ERROR') OR tb.duration ::FLOAT <= 0 OR tb.status IN ('SKIP', 'ERROR') THEN ts.status
             WHEN (tb.duration >= 100 AND ts.duration::FLOAT / tb.duration::FLOAT BETWEEN 0.9 AND 1.1) OR (tb.duration < 100 AND ts.duration::FLOAT / tb.duration::FLOAT BETWEEN 0.3 AND 3)THEN 'PASS'
             WHEN (tb.duration >= 100 AND ts.duration::FLOAT / tb.duration::FLOAT < 0.9) OR (tb.duration <= 100 AND ts.duration::FLOAT / tb.duration::FLOAT < 0.3) THEN 'PASS WITH PERFORMANCE IMPROVEMENT'
             WHEN (tb.duration >= 100 AND ts.duration::FLOAT / tb.duration::FLOAT > 1.1) OR (tb.duration <= 100 AND ts.duration::FLOAT / tb.duration::FLOAT > 3) THEN 'FAILURE WITH PERFORMANCE DOWNGRADE'
             ELSE  'ERROR WITH TEST REPORT GENERATION'::TEXT
        END AS test_result,

        CASE WHEN tb.status IS NULL OR tb.duration = 0 OR ts.status IN ('SKIP', 'ERROR') OR ts.duration IS NULL THEN NULL
             ELSE (ts.duration::FLOAT / tb.duration::FLOAT)::DECIMAL(18,2)
        END AS deviation
        FROM actual_test_result_table AS ts
    LEFT JOIN baseline_test_result_table AS tb
    ON ts.adj_s_id = tb.s_id AND ts.action_type = tb.action_type AND ts.action_target = tb.action_target
    WHERE ts.wl_disrandomly is TRUE;
    

    RETURN QUERY
       SELECT tr.tr_id, tr.s_id, tr.wl_name, tr.action_type, tr.action_target, tr.actual_execution_time, tr.baseline_execution_time,
              tr.test_result, tr.deviation,
             CASE WHEN tr.test_result LIKE 'SKIP%' OR tr.test_result LIKE 'ERROR%' OR tr.baseline_execution_time  IS NULL THEN 0
                  ELSE tr.baseline_execution_time
             END AS revised_baseline_execution_time,

             CASE WHEN tr.test_result IN ('PASS', 'PASS AS NEW TEST CASE') THEN 1 ELSE 0
             END AS success_status,

             CASE WHEN tr.test_result IN ('PASS WITH PERFORMANCE IMPROVEMENT') THEN 1 ELSE 0
             END AS improve_status,
             
             CASE WHEN tr.test_result LIKE 'FAILURE%' THEN 1 ELSE 0
             END AS failure_status,

             CASE WHEN tr.test_result IN ('SKIP') THEN 1 ELSE 0
             END AS skip_status,

             CASE WHEN tr.test_result LIKE 'ERROR%' THEN 1 ELSE 0
             END AS error_status
        FROM tmp_test_report_detail AS tr;
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_generate_test_report_detail(start_run_id INT, end_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), actual_execution_time DECIMAL(18,2), baseline_execution_time DECIMAL(18,2), test_result TEXT, deviation DECIMAL(18,2), revised_baseline_execution_time DECIMAL(18,2), success_status INT, improve_status INT, failure_status INT, skip_status INT, error_status INT)
AS $$
DECLARE
    actual_test_result_table TEXT;
    baseline_test_result_table TEXT;
BEGIN
    CREATE TEMP TABLE actual_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_result(start_run_id, end_run_id);

    CREATE TEMP TABLE baseline_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_baseline(baseline_hdfs_version, baseline_hawq_version);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_detail_internal(actual_test_result_table, baseline_test_result_table);
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_detail(test_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), actual_execution_time DECIMAL(18,2), baseline_execution_time DECIMAL(18,2), test_result TEXT, deviation DECIMAL(18,2), revised_baseline_execution_time DECIMAL(18,2), success_status INT, improve_status INT, failure_status INT, skip_status INT, error_status INT)
AS $$
DECLARE
    actual_test_result_table TEXT;
    baseline_test_result_table TEXT;
BEGIN
    CREATE TEMP TABLE actual_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_result(test_run_id);

    CREATE TEMP TABLE baseline_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_baseline(baseline_hdfs_version, baseline_hawq_version);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_detail_internal(actual_test_result_table, baseline_test_result_table);
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_detail(test_run_id_actual INT, test_run_id_baseline INT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), actual_execution_time DECIMAL(18,2), baseline_execution_time DECIMAL(18,2), test_result TEXT, deviation DECIMAL(18,2), revised_baseline_execution_time DECIMAL(18,2), success_status INT, improve_status INT, failure_status INT, skip_status INT, error_status INT)
AS $$
DECLARE
    actual_test_result_table TEXT;
    baseline_test_result_table TEXT;
BEGIN
    CREATE TEMP TABLE actual_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_result(test_run_id_actual);

    CREATE TEMP TABLE baseline_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_result(test_run_id_baseline);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_detail_internal(actual_test_result_table, baseline_test_result_table);
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_generate_test_report_detail(start_run_id_actual INT, end_run_id_actual INT, start_run_id_baseline INT, end_run_id_baseline INTG)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), actual_execution_time DECIMAL(18,2), baseline_execution_time DECIMAL(18,2), test_result TEXT, deviation DECIMAL(18,2), revised_baseline_execution_time DECIMAL(18,2), success_status INT, improve_status INT, failure_status INT, skip_status INT, error_status INT)
AS $$
DECLARE
    actual_test_result_table TEXT;
    baseline_test_result_table TEXT;
BEGIN
    CREATE TEMP TABLE actual_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_result(start_run_id_actual, end_run_id_actual);

    CREATE TEMP TABLE baseline_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_result(start_run_id_baseline, end_run_id_baseline);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_detail_internal(actual_test_result_table, baseline_test_result_table);
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_detail(baseline1_hdfs_version TEXT, baseline1_hawq_version TEXT, baseline2_hdfs_version TEXT, baseline2_hawq_version TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), action_target VARCHAR(128), actual_execution_time DECIMAL(18,2), baseline_execution_time DECIMAL(18,2), test_result TEXT, deviation DECIMAL(18,2), revised_baseline_execution_time DECIMAL(18,2), success_status INT, improve_status INT, failure_status INT, skip_status INT, error_status INT)
AS $$
DECLARE
    actual_test_result_table TEXT;
    baseline_test_result_table TEXT;
BEGIN
    CREATE TEMP TABLE actual_test_result_table ON COMMIT DROP AS
    SELECT NULL::INT AS tr_id, * FROM hst.f_generate_test_baseline(baseline1_hdfs_version, baseline1_hawq_version);

    CREATE TEMP TABLE baseline_test_result_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_baseline(baseline2_hdfs_version, baseline2_hawq_version);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_detail_internal(actual_test_result_table, baseline_test_result_table);
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_summary_internal(test_report_table TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), test_statistic TEXT, 
              improvenum int, passnum int, failurenum int, skipnum int, errornum int,
              actual_total_execution_time DECIMAL(18,2), baseline_total_execution_time DECIMAL(18,2), deviation DECIMAL(18,2), overral_test_result TEXT, detail_result TEXT, test_result_all TEXT)
AS $$
BEGIN
    CREATE TEMP TABLE tmp_test_report_summary ON COMMIT DROP AS
    SELECT ts.tr_id, ts.s_id, ts.wl_name, ts.action_type,
           ('Detail(IMPROVEMENT: ' || SUM(ts.improve_status)::TEXT || '; PASS: ' || SUM(ts.success_status)::TEXT || '; FAILURE: ' || SUM(failure_status)::TEXT || '; SKIP: ' || SUM(ts.skip_status)::TEXT || '; ERROR: ' || SUM(ts.error_status)::TEXT)  || ')' AS test_statistic,
           SUM(ts.improve_status)::int as improvenum, SUM(ts.success_status)::int as passnum,
           SUM(ts.failure_status)::int as failurenum, SUM(ts.skip_status)::int as skipnum,
           SUM(ts.error_status)::int as errornum, 
           SUM(ts.actual_execution_time)::DECIMAL(18,2) AS actual_total_execution_time,  
           SUM(ts.revised_baseline_execution_time)::DECIMAL(18,2) AS baseline_total_execution_time,  
           CASE WHEN SUM(ts.revised_baseline_execution_time) < 0.001 THEN NULL
                ELSE (SUM(ts.actual_execution_time)::FLOAT/SUM(ts.revised_baseline_execution_time)::FLOAT)::DECIMAL(18,2) END AS deviation,
           CASE WHEN SUM(ts.error_status) > 0 THEN 'ERROR'
                WHEN SUM(ts.actual_execution_time) < 0.001 OR SUM(ts.revised_baseline_execution_time) < 0.001 THEN 'SKIP'
                WHEN SUM(ts.actual_execution_time)::FLOAT / SUM(ts.revised_baseline_execution_time)::FLOAT BETWEEN 0.9 AND 1.1 THEN 'PASS' 
                WHEN SUM(ts.actual_execution_time)::FLOAT / SUM(ts.revised_baseline_execution_time)::FLOAT < 0.9  THEN 'PASS WITH PERFORMANCE IMPROVEMENT' 
                WHEN SUM(ts.actual_execution_time)::FLOAT / SUM(ts.revised_baseline_execution_time)::FLOAT > 1.1 THEN 'FAILURE WITH PERFORMANCE DOWNGRADE'
                ELSE  'ERROR'
           END AS overall_test_result,
           CASE WHEN SUM(ts.error_status) > 0 THEN 'ERROR' 
                WHEN SUM(ts.failure_status) > 0 THEN 'FAILURE'
                WHEN SUM(ts.success_status) + SUM(ts.improve_status) + SUM(failure_status) +SUM(ts.error_status) = 0 THEN 'SKIP'
                ELSE 'PASS' END AS detail_result
    FROM test_report_table AS ts
    WHERE action_target not like '%_STREAM'
    GROUP BY ts.tr_id, ts.s_id, ts.wl_name, ts.action_type;


    RETURN QUERY
        SELECT ts.tr_id, ts.s_id, ts.wl_name, ts.action_type,
               ('Overall:' || ts.overall_test_result || ';     '|| ts.test_statistic) AS test_statistic,
               ts.improvenum, ts.passnum, ts.failurenum, ts.skipnum, ts.errornum,
                ts.actual_total_execution_time , ts.baseline_total_execution_time, ts.deviation,
               ts.overall_test_result, ts.detail_result,
               CASE WHEN ts.overall_test_result IN ('SKIP', 'ERROR') THEN ts.overall_test_result
                    WHEN ts.overall_test_result like 'PASS%' AND ts.detail_result like 'PASS%' THEN 'PASS'
                    ELSE 'FAILURE' END AS test_result_all
        FROM tmp_test_report_summary AS ts;

END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_generate_test_report_summary(start_run_id INT,end_run_id INT,  baseline_hdfs_version TEXT, baseline_hawq_version TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), test_statistic TEXT,
improvenum int, passnum int, failurenum int, skipnum int, errornum int,
actual_total_execution_time DECIMAL(18,2), baseline_total_execution_time DECIMAL(18,2), deviation DECIMAL(18,2),  overral_test_result TEXT, detail_result TEXT, test_result_all TEXT)
AS $$
DECLARE
    test_report_table TEXT;
BEGIN
    CREATE TEMP TABLE test_report_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_report_detail(start_run_id, end_run_id, baseline_hdfs_version, baseline_hawq_version);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_summary_internal(test_report_table);
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_summary(test_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), test_statistic TEXT, 
improvenum int, passnum int, failurenum int, skipnum int, errornum int,
actual_total_execution_time DECIMAL(18,2), baseline_total_execution_time DECIMAL(18,2), deviation DECIMAL(18,2),  overral_test_result TEXT, detail_result TEXT, test_result_all TEXT)
AS $$
DECLARE
    test_report_table TEXT;
BEGIN
    CREATE TEMP TABLE test_report_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_report_detail(test_run_id, baseline_hdfs_version, baseline_hawq_version);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_summary_internal(test_report_table);
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_summary(test_run_id_actual INT, test_run_id_baseline INT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), test_statistic TEXT, 
improvenum int, passnum int, failurenum int, skipnum int, errornum int,
actual_total_execution_time DECIMAL(18,2), baseline_total_execution_time DECIMAL(18,2), deviation DECIMAL(18,2),  overral_test_result TEXT, detail_result TEXT, test_result_all TEXT)
AS $$
DECLARE
    test_report_table TEXT;
BEGIN
    CREATE TEMP TABLE test_report_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_report_detail(test_run_id_actual, test_run_id_baseline);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_summary_internal(test_report_table);
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_generate_test_report_summary(start_run_id INT, end_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), test_statistic TEXT,
improvenum int, passnum int, failurenum int, skipnum int, errornum int,
actual_total_execution_time DECIMAL(18,2), baseline_total_execution_time DECIMAL(18,2), deviation DECIMAL(18,2),  overral_test_result TEXT, detail_result TEXT, test_result_all TEXT)
AS $$
DECLARE
    test_report_table TEXT;
BEGIN
    CREATE TEMP TABLE test_report_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_report_detail(start_run_id, end_run_id, baseline_hdfs_version, baseline_hawq_version);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_summary_internal(test_report_table);
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hst.f_generate_test_report_summary(start_run_id_actual INT,end_run_id_actual INT, start_run_id_baseline INT,end_run_id_baseline INT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), test_statistic TEXT,
improvenum int, passnum int, failurenum int, skipnum int, errornum int,
actual_total_execution_time DECIMAL(18,2), baseline_total_execution_time DECIMAL(18,2), deviation DECIMAL(18,2),  overral_test_result TEXT, detail_result TEXT, test_result_all TEXT)
AS $$
DECLARE
    test_report_table TEXT;
BEGIN
    CREATE TEMP TABLE test_report_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_report_detail(start_run_id_actual,end_run_id_actual, start_run_id_baseline,end_run_id_baseline);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_summary_internal(test_report_table);
END
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION hst.f_generate_test_report_summary(baseline1_hdfs_version TEXT, baseline1_hawq_version TEXT, baseline2_hdfs_version TEXT, baseline2_hawq_version TEXT)
RETURNS TABLE(tr_id INT, s_id INT, wl_name VARCHAR(512), action_type VARCHAR(128), test_statistic TEXT, 
improvenum int, passnum int, failurenum int, skipnum int, errornum int,
actual_total_execution_time DECIMAL(18,2), baseline_total_execution_time DECIMAL(18,2), deviation DECIMAL(18,2),  overral_test_result TEXT, detail_result TEXT, test_result_all TEXT)
AS $$
DECLARE
    test_report_table TEXT;
BEGIN
    CREATE TEMP TABLE test_report_table ON COMMIT DROP AS
    SELECT * FROM hst.f_generate_test_report_detail(baseline1_hdfs_version, baseline1_hawq_version, baseline2_hdfs_version, baseline2_hawq_version);

    RETURN QUERY SELECT * FROM hst.f_generate_test_report_summary_internal(test_report_table);
END
$$ LANGUAGE PLPGSQL;



--
-- 4. Grant access privileges
-- 
GRANT ALL ON SCHEMA hst TO hawq_cov;

GRANT ALL ON TABLE hst.test_result TO hawq_cov;
GRANT ALL ON TABLE hst.test_baseline TO hawq_cov;
GRANT ALL ON TABLE hst.test_run TO hawq_cov;

GRANT ALL ON TABLE hst.scenario TO hawq_cov;

GRANT ALL ON TABLE hst.cluster_settings TO hawq_cov;
GRANT ALL ON TABLE hst.hdfs_settings TO hawq_cov;
GRANT ALL ON TABLE hst.hawq_settings TO hawq_cov;

GRANT ALL ON TABLE hst.workload TO hawq_cov;
GRANT ALL ON TABLE hst.users TO hawq_cov;
GRANT ALL ON TABLE hst.resource_queue TO hawq_cov;


GRANT ALL ON FUNCTION hst.f_precompute_test_baseline(baseline_hdfs_version TEXT, baseline_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_baseline(baseline_hdfs_version TEXT, baseline_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_precompute_test_result(test_run_id INT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_result(test_run_id INT) TO hawq_cov;

GRANT ALL ON FUNCTION hst.f_generate_test_report_detail_internal(actual_test_result_table TEXT, baseline_test_result_table TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_detail(test_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_detail(test_run_id_actual INT, test_run_id_baseline INT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_detail(baseline1_hdfs_version TEXT, baseline1_hawq_version TEXT, baseline2_hdfs_version TEXT, baseline2_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_detail(start_run_id INT, end_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_detail(start_run_id_actual INT,end_run_id_actual INT, start_run_id_baseline INT,end_run_id_baseline INT) TO hawq_cov;

GRANT ALL ON FUNCTION hst.f_generate_test_report_summary_internal(test_report_table TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_summary(test_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_summary(start_run_id INT, end_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_summary(test_run_id_actual INT, test_run_id_baseline INT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_summary(baseline1_hdfs_version TEXT, baseline1_hawq_version TEXT, baseline2_hdfs_version TEXT, baseline2_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_summary(start_run_id INT, end_run_id INT, baseline_hdfs_version TEXT, baseline_hawq_version TEXT) TO hawq_cov;
GRANT ALL ON FUNCTION hst.f_generate_test_report_summary(start_run_id_actual INT,end_run_id_actual INT, start_run_id_baseline INT,end_run_id_baseline INT) TO hawq_cov;

--
-- 5. Metadata for bootstrap
--
INSERT INTO hst.hdfs_settings (hds_security, hds_ha, hds_description) VALUES (TRUE, TRUE, 'Default HDFS settings');
INSERT INTO hst.hawq_settings (hqs_num_segment, hqs_kerberos, hqs_description) VALUES (4, TRUE, 'Default HAWQ settings');
INSERT INTO hst.cluster_settings (cs_name, hds_id, hqs_id, cs_cpu, cs_memory, cs_disk, cs_num_node, cs_hdfs_roles, cs_hawq_roles, cs_description) VALUES ('HAWQ main performance on dca22', 1, 1, 'Intel(R) Xeon(R) CPU E5-2660 0 @ 2.20GHz', '64G', '8.2T * 2 Disks * 16 Nodes', 18, 'mdw: standby namenode, ZKFC; smdw: active namenode, ZKFC; sdw1~sdw3: datanode, zookeeper; sdw4~sdw6: datanode, journalnode; sdw7~sdw16: datanode', 'mdw: master; smdw: standby master; sdw1~sdw16: 4 segments per node', 'HAWQ main performance on dca22');
INSERT INTO hst.workload (wl_name, wl_catetory, wl_data_volume_type, wl_data_volume_size, wl_appendonly, wl_orientation, wl_row_group_size, wl_page_size, wl_compression_type, wl_compression_level, wl_partitions, wl_iteration, wl_concurrency, wl_query_order) VALUES ('tpch_parquet_10gpn_snappy_nopart', 'TPCH', 'PER_SEGMENT', 10, TRUE, 'PARQUET', 8388608, 1048576, 'SNAPPY', NULL, 0, 1, 1, 'SEQUENTIAL');
INSERT INTO hst.resource_queue (rq_name, rq_definition) VALUES ('Default HAWQ resource queue', '');
INSERT INTO hst.users (us_name, rq_id) VALUES ('gpadmin', 1);
INSERT INTO hst.scenario (cs_id, wl_id, us_id) VALUES (1, 1, 1);



-- 6. Sample for reporting
SELECT * FROM hst.f_precompute_test_baseline('PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');
SELECT * FROM hst.f_generate_test_baseline('PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');


SELECT * FROM hst.f_precompute_test_result(233);
SELECT * FROM hst.f_precompute_test_result(237);
SELECT * FROM hst.f_generate_test_result(233);
SELECT * FROM hst.f_generate_test_result(237);

SELECT * FROM hst.f_generate_test_report_detail(233, 'PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');
SELECT * FROM hst.f_generate_test_report_detail(233, 233);
SELECT * FROM hst.f_generate_test_report_detail('PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946','PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');

SELECT * FROM hst.f_generate_test_report_detail(233, 237, 'PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');
SELECT * FROM hst.f_generate_test_report_detail(233, 237,233,237);

SELECT * FROM hst.f_generate_test_report_summary(233, 'PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');
SELECT * FROM hst.f_generate_test_report_summary(89, 'PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');
SELECT * FROM hst.f_generate_test_report_summary(87, 'PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946');
SELECT * FROM hst.f_generate_test_report_summary(87, 87);
SELECT * FROM hst.f_generate_test_report_summary('PHD 2.2', 'HAWQ 1.2.1.2 build 11946','PHD 2.2', 'HAWQ 1.2.1.2 build 11946');


--Generate baseline
INSERT INTO hst.test_baseline SELECT
'PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946 GVA ORCA OFF',
s_id, action_type,action_target,iteration,stream,status, start_time,end_time,duration,output,plan,resource_usage,adj_s_id
from test_result where tr_id in(300,301,302);


