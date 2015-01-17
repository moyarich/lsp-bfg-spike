--1. Generate Time slot for each query

TRUNCATE test_result_info;

INSERT INTO test_result_info
SELECT 
CASE WHEN w.wl_concurrency > 1 THEN (w.wl_name || '_CONCURRENT')::VARCHAR(512)
    ELSE w.wl_name END wl_name, ts.action_target, ts.stream, ts.start_time,ts.end_time, con_id
FROM hst.test_result AS ts, scenario s,workload w
WHERE ts.s_id= s.s_id and w.wl_id = s.wl_id
      AND ts.status != 'SKIP'
      AND ts.tr_id in (87,88,89);

--2. Generate cpu and memory data for each QE per connection
DROP TABLE IF EXISTS qe_mem_cpu_per_seg_con CASCADE;

CREATE TABLE qe_mem_cpu_per_seg_con AS
SELECT run_id, hostname, timeslot, 
min(real_time) as begintime,con_id, seg_id,
SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
FROM qe_mem_cpu
WHERE run_id in(1,2,3)
GROUP BY run_id, hostname, timeslot, con_id, seg_id ;

--3. Generate cpu and memory data for each QD  per connection

DROP TABLE IF EXISTS qd_mem_cpu_per_con CASCADE;

CREATE TABLE qd_mem_cpu_per_con  AS
SELECT run_id, hostname, timeslot, 
min(real_time) as begintime, con_id,
SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
FROM qd_mem_cpu
WHERE run_id in(1,2,3)
GROUP BY run_id, hostname, timeslot, con_id;

--4. Generate qd&qe nodes level information

DROP table if exists qde_mem_cpu_per_node;

CREATE table qde_mem_cpu_per_node AS
SELECT hostname, 'QE' as role, timeslot, begintime,
SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
FROM qe_mem_cpu_per_seg_con
GROUP BY hostname,timeslot, begintime
UNION
SELECT hostname, 'QD' as role, timeslot, begintime,
sum(rss), sum(pmem), sum(pcpu) 
FROM qd_mem_cpu_per_con
GROUP BY hostname, timeslot, begintime;

--5. Generate QD/QE for query base 

SELECT f_generate_query_stat(); 


CREATE OR REPLACE FUNCTION f_liuq(start_id int, end_id int)
RETURNS INTEGER
AS $$
BEGIN
  TRUNCATE test_result_info;
  INSERT INTO test_result_info
  SELECT 
  CASE WHEN w.wl_concurrency > 1 THEN (w.wl_name || '_CONCURRENT')::VARCHAR(512)
       ELSE w.wl_name END wl_name, ts.action_target, ts.stream, ts.start_time, ts.end_time, con_id
  FROM hst.test_result AS ts, scenario s, workload w
  WHERE ts.s_id= s.s_id and w.wl_id = s.wl_id
        AND ts.status != 'SKIP'
        AND ts.tr_id >= start_id AND ts.tr_id <= end_id ;
  
  DROP TABLE IF EXISTS qe_mem_cpu_per_seg_con CASCADE;
  CREATE TABLE qe_mem_cpu_per_seg_con AS
  SELECT  run_id, hostname, timeslot, 
      min(real_time) as begintime,con_id, seg_id,
      SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
  FROM qe_mem_cpu
  WHERE run_id >= start_id AND run_id <= end_id
  GROUP BY run_id, hostname, timeslot, con_id, seg_id ;
  
  DROP TABLE IF EXISTS qd_mem_cpu_per_con CASCADE;
  CREATE TABLE qd_mem_cpu_per_con AS
  SELECT run_id, hostname, timeslot, 
    min(real_time) as begintime, con_id,
    SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
  FROM qd_mem_cpu
  WHERE run_id >= start_id AND run_id <= end_id
  GROUP BY run_id, hostname, timeslot, con_id;
  
  DROP table if exists qde_mem_cpu_per_node;
  CREATE table qde_mem_cpu_per_node AS
    SELECT hostname, 'QE' as role, timeslot, begintime,
      SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
    FROM qe_mem_cpu_per_seg_con
    GROUP BY hostname,timeslot, begintime
    UNION
    SELECT hostname, 'QD' as role, timeslot, begintime,
      sum(rss), sum(pmem), sum(pcpu) 
    FROM qd_mem_cpu_per_con
    GROUP BY hostname, timeslot, begintime;

  INSERT INTO hst.qd_info_history select * from hst.qd_info;
  TRUNCATE TABLE hst.qd_info;

  INSERT INTO hst.qd_mem_cpu_history select * from hst.qd_mem_cpu;
  TRUNCATE TABLE hst.qd_mem_cpu;

  INSERT INTO hst.qe_mem_cpu_history select * from hst.qe_mem_cpu;
  TRUNCATE TABLE hst.qe_mem_cpu;
  
RETURN 0;
END
$$ LANGUAGE PLPGSQL;

INSERT INTO hst.qd_info select * from hst.qd_info_history;
TRUNCATE TABLE hst.qd_info_history;

INSERT INTO hst.qd_mem_cpu select * from hst.qd_mem_cpu_history;
TRUNCATE TABLE hst.qd_mem_cpu_history;

INSERT INTO hst.qe_mem_cpu select * from hst.qe_mem_cpu_history;
TRUNCATE TABLE hst.qe_mem_cpu_history;

SELECT f_generate_query_stat();