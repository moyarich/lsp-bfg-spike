-------------------
Schema Creation
---------------------
DROP TABLE IF EXISTS test_result_info;
CREATE TABLE test_result_info 
(wl_name varchar(512),
 action_target varchar(128),
 stream int,
 start_time  timestamp with time zone,
 end_time timestamp with time zone,
 con_id int);

DROP TABLE IF EXISTS workload_run_info;
CREATE TABLE workload_run_info 
(wl_name varchar(512),
 start_time  timestamp with time zone,
 end_time timestamp with time zone);


DROP TABLE IF EXISTS query_seg_stat_info;
DROP TABLE IF EXISTS query_master_stat_info;
CREATE TABLE query_seg_stat_info (
        workload_name varchar(512),
        query_name varchar(512),
        stream int,
        segnum int,
        pmem_max  decimal(8,2),
        pmem_avg   decimal(8,2),
        pmem_min   decimal(8,2), 
        pmemratio    decimal(8,2),
        pcpu_max    decimal(8,2),
        pcpu_avg    decimal(8,2),
        pcpu_min    decimal(8,2),
        pcpuratio     decimal(8,2),
        rss_max      decimal(8,2),
        rss_avg       decimal(8,2),
        rss_min      decimal(8,2),
        rssratio      decimal(8,2));
 
CREATE TABLE query_master_stat_info (
        workload_name varchar(512),
        query_name varchar(512),
        stream int,
        pmem_max  decimal(8,2),
        pcpu_max    decimal(8,2),
        rss_max      decimal(8,2)
        );
        

-------------------
Data Preparion
1. Generate Time slot for each query
2. Generate cpu and memory data for each QE per connection
3. Generate cpu and memory data for each QD  per connection
4. Generate qd&qe nodes level information
5. Generate QD/QE for query base 

---------------------
DROP FUNCTION if exists hst.f_generate_monitor_report(int, int, boolean);
CREATE OR REPLACE FUNCTION hst.f_generate_monitor_report(start_id int, end_id int, isclear boolean)
RETURNS INTEGER
AS $$
BEGIN
  set search_path = hst;
  TRUNCATE test_result_info;
  INSERT INTO test_result_info
    SELECT 
      CASE WHEN w.wl_concurrency > 1 THEN (w.wl_name || '_CONCURRENT')::VARCHAR(512)
      ELSE w.wl_name END wl_name, ts.action_target, ts.stream, ts.start_time, ts.end_time, con_id
    FROM hst.test_result AS ts, scenario s, workload w
    WHERE ts.s_id= s.s_id and w.wl_id = s.wl_id
          AND ts.status != 'SKIP'
          AND ts.tr_id >= start_id AND ts.tr_id <= end_id ;

  TRUNCATE workload_run_info
  INSERT INTO workload_run_info
    SELECT
      CASE WHEN w.wl_concurrency > 1 THEN (w.wl_name || '_CONCURRENT')::VARCHAR(512)
      ELSE w.wl_name END wl_name,MIN(ts.start_time) as start_time, MAX(ts.end_time) AS end_time
    FROM hst.test_result AS ts, scenario s,workload w
    WHERE ts.s_id= s.s_id and w.wl_id = s.wl_id
      AND ts.status != 'SKIP'
      AND ts.tr_id >= start_id AND ts.tr_id <= end_id 
    GROUP BY  w.wl_name,w.wl_concurrency;
  
  TRUNCATE qe_mem_cpu_per_seg_con;
  INSERT INTO qe_mem_cpu_per_seg_con
    SELECT run_id, hostname, timeslot, 
      min(real_time) as begintime,con_id, seg_id,
      SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
    FROM qe_mem_cpu
    WHERE run_id >= start_id AND run_id <= end_id
    GROUP BY run_id, hostname, timeslot, con_id, seg_id ;
  
  TRUNCATE qd_mem_cpu_per_con;
  INSERT INTO qd_mem_cpu_per_con
    SELECT run_id, hostname, timeslot, 
      min(real_time) as begintime, con_id,
      SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
    FROM qd_mem_cpu
    WHERE run_id >= start_id AND run_id <= end_id
    GROUP BY run_id, hostname, timeslot, con_id;
  
  TRUNCATE qde_mem_cpu_per_node;
  INSERT INTO qde_mem_cpu_per_node
    SELECT hostname, 'QE' as role, timeslot, begintime,
      SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
    FROM qe_mem_cpu_per_seg_con
    GROUP BY hostname,timeslot, begintime
    UNION
    SELECT hostname, 'QD' as role, timeslot, begintime,
      sum(rss), sum(pmem), sum(pcpu) 
    FROM qd_mem_cpu_per_con
    GROUP BY hostname, timeslot, begintime;

  PERFORM f_generate_query_stat();

  IF isclear THEN
    INSERT INTO hst.qd_info_history select * from hst.qd_info WHERE run_id in (116,117,118,119);
    TRUNCATE TABLE hst.qd_info;

    INSERT INTO hst.qd_mem_cpu_history select * from hst.qd_mem_cpu;
    TRUNCATE TABLE hst.qd_mem_cpu;

    INSERT INTO hst.qe_mem_cpu_history select * from hst.qe_mem_cpu;
    TRUNCATE TABLE hst.qe_mem_cpu;
  END IF;

RETURN 0;
END
$$ LANGUAGE PLPGSQL;

--------
DATA Analysis Per Query
1. Get query start time and end time
2. Get stat info between start time and end time
3. Get info 
    For Segment:1) get segment count 2) get max/avg/min usage for those segments 
    For Master 1) get useage for master
----------------
DROP FUNCTION IF EXISTS f_generate_query_stat();
CREATE OR REPLACE FUNCTION f_generate_query_stat()
RETURNS INTEGER
AS $$
DECLARE 
      qe_cur REFCURSOR;
      v_count int;
      v_i int;
        wl_name varchar(512);
        query_name varchar(512);
        stream int;
        start_time timestamp with time zone;
        end_time timestamp with time zone;
BEGIN
      v_i := 1;
        TRUNCATE query_seg_stat_info;
        TRUNCATE query_master_stat_info;
        SELECT COUNT(*) INTO v_count FROM test_result_info;
        OPEN qe_cur FOR SELECT * FROM test_result_info;
        WHILE v_i < v_count loop
          FETCH qe_cur INTO wl_name, query_name, stream, start_time, end_time;
            INSERT INTO query_seg_stat_info 
            SELECT  wl_name, query_name,stream,
              COUNT(*) AS segnum,
                max(qes.pmem)::decimal(8,2) as pmem_max, 
                avg(qes.pmem)::decimal(8,2) as pmem_avg, 
                min(qes.pmem)::decimal(8,2) as pmem_min, 
                CASE WHEN min(qes.pmem) = 0 THEN 0 ELSE (max(qes.pmem) / min(qes.pmem))::decimal(8,2) END as pmemratio,
                max(qes.pcpu)::decimal(8,2) as pcpu_max,
                avg(qes.pcpu)::decimal(8,2) as pcpu_avg,
                min(qes.pcpu)::decimal(8,2) as pcpu_min,  
                CASE WHEN min(qes.pcpu) = 0 THEN 0 ELSE (max(qes.pcpu) / min(qes.pcpu))::decimal(8,2) END as pcpuratio,
                max(qes.rss)::decimal(8,2)  as rss_max,
                avg(qes.rss)::decimal(8,2)  as rss_avg,
                min(qes.rss)::decimal(8,2) as rss_min, 
                CASE WHEN min(qes.rss) = 0 THEN 0 ELSE  (max(qes.rss) / min(qes.rss))::decimal(8,2)  END as rssratio
             FROM ( SELECT qe.hostname||'-' ||qe.seg_id, 
                           max(qe.pmem) as pmem, 
                           max(qe.pcpu) as pcpu, 
                           max(qe.rss) as rss 
                    FROM qe_mem_cpu_per_seg_con AS qe
                    WHERE  qe.begintime >=  start_time AND qe.begintime <  end_time 
                    GROUP BY qe.hostname||'-'||qe.seg_id) AS qes;
             INSERT INTO query_master_stat_info 
             SELECT  wl_name, query_name,stream,
                     max(qd.pmem) as pmem, 
                     max(qd.pcpu) as pcpu, 
                     max(qd.rss) as rss 
             FROM qd_mem_cpu_per_con as qd
             WHERE  qd.begintime >=  start_time AND qd.begintime <  end_time ;
             v_i = v_i + 1;
      END LOOP;
      CLOSE qe_cur;
RETURN v_i;
END
$$ LANGUAGE PLPGSQL;
SELECT f_generate_query_stat();
SELECT * FROM query_seg_stat_info;
SELECT * FROM query_master_stat_info;


---------------------------For each node get trend
drop function f_get_host_memory(workloadname varchar(512));
CREATE OR REPLACE FUNCTION f_get_host_memory(workloadname varchar(512))
RETURNS TABLE(timeslot int, mst1 numeric, w1 numeric, w2 numeric, w3 numeric, w4 numeric, w5 numeric, w6 numeric, w7 numeric, w8 numeric, w9 numeric, 
w10 numeric,w11 numeric, w12 numeric, w13 numeric, w14 numeric, w15 numeric, w16 numeric)
AS $$
DECLARE
   start_time timestamp with time zone;
   end_time timestamp with time zone;
BEGIN
    SELECT wl.start_time into start_time
    FROM workload_run_info as wl
    WHERE wl.wl_name = workloadname;
    SELECT wl.end_time into end_time
    FROM workload_run_info as wl
    WHERE wl.wl_name = workloadname;
    RETURN QUERY
  SELECT qde.timeslot, 
    MAX(CASE WHEN qde.hostname = 'bcn-mst1' THEN qde.rss ELSE 0 END) as mst1,
    MAX(CASE WHEN qde.hostname = 'bcn-w1' THEN qde.rss ELSE 0 END) as w1,
    MAX(CASE WHEN qde.hostname = 'bcn-w2' THEN qde.rss ELSE 0 END) as w2,
    MAX(CASE WHEN qde.hostname = 'bcn-w3' THEN qde.rss ELSE 0 END) as w3,
    MAX(CASE WHEN qde.hostname = 'bcn-w4' THEN qde.rss ELSE 0 END) as w4,
    MAX(CASE WHEN qde.hostname = 'bcn-w5' THEN qde.rss ELSE 0 END) as w5,
    MAX(CASE WHEN qde.hostname = 'bcn-w6' THEN qde.rss ELSE 0 END) as w6,
    MAX(CASE WHEN qde.hostname = 'bcn-w7' THEN qde.rss ELSE 0 END) as w7,
    MAX(CASE WHEN qde.hostname = 'bcn-w8' THEN qde.rss ELSE 0 END) as w8,
    MAX(CASE WHEN qde.hostname = 'bcn-w9' THEN qde.rss ELSE 0 END) as w9,
    MAX(CASE WHEN qde.hostname = 'bcn-w10' THEN qde.rss ELSE 0 END) as w10,
    MAX(CASE WHEN qde.hostname = 'bcn-w11' THEN qde.rss ELSE 0 END) as w11,
    MAX(CASE WHEN qde.hostname = 'bcn-w12' THEN qde.rss ELSE 0 END) as w12,
    MAX(CASE WHEN qde.hostname = 'bcn-w13' THEN qde.rss ELSE 0 END) as w13,
    MAX(CASE WHEN qde.hostname = 'bcn-w14' THEN qde.rss ELSE 0 END) as w14,
    MAX(CASE WHEN qde.hostname = 'bcn-w15' THEN qde.rss ELSE 0 END) as w15,
    MAX(CASE WHEN qde.hostname = 'bcn-w16' THEN qde.rss ELSE 0 END) as w16
  FROM qde_mem_cpu_per_node as qde
  WHERE qde.begintime BETWEEN start_time AND end_time
  GROUP BY qde.timeslot;
END
$$ LANGUAGE PLPGSQL;


drop function f_get_host_cpu(workloadname varchar(512));
CREATE OR REPLACE FUNCTION f_get_host_cpu(workloadname varchar(512))
RETURNS TABLE(timeslot int, mst1 numeric, w1 numeric, w2 numeric, w3 numeric, w4 numeric, w5 numeric, w6 numeric, w7 numeric, w8 numeric, w9 numeric, 
w10 numeric,w11 numeric, w12 numeric, w13 numeric, w14 numeric, w15 numeric, w16 numeric)
AS $$
DECLARE
   start_time timestamp with time zone;
   end_time timestamp with time zone;
BEGIN
    SELECT wl.start_time into start_time
    FROM workload_run_info as wl
    WHERE wl.wl_name = workloadname;
    SELECT wl.end_time into end_time
    FROM workload_run_info as wl
    WHERE wl.wl_name = workloadname;
    RETURN QUERY
  SELECT qde.timeslot, 
    MAX(CASE WHEN qde.hostname = 'bcn-mst1' THEN qde.pcpu ELSE 0 END) as mst1,
    MAX(CASE WHEN qde.hostname = 'bcn-w1' THEN qde.pcpu ELSE 0 END) as w1,
    MAX(CASE WHEN qde.hostname = 'bcn-w2' THEN qde.pcpu ELSE 0 END) as w2,
    MAX(CASE WHEN qde.hostname = 'bcn-w3' THEN qde.pcpu ELSE 0 END) as w3,
    MAX(CASE WHEN qde.hostname = 'bcn-w4' THEN qde.pcpu ELSE 0 END) as w4,
    MAX(CASE WHEN qde.hostname = 'bcn-w5' THEN qde.pcpu ELSE 0 END) as w5,
    MAX(CASE WHEN qde.hostname = 'bcn-w6' THEN qde.pcpu ELSE 0 END) as w6,
    MAX(CASE WHEN qde.hostname = 'bcn-w7' THEN qde.pcpu ELSE 0 END) as w7,
    MAX(CASE WHEN qde.hostname = 'bcn-w8' THEN qde.pcpu ELSE 0 END) as w8,
    MAX(CASE WHEN qde.hostname = 'bcn-w9' THEN qde.pcpu ELSE 0 END) as w9,
    MAX(CASE WHEN qde.hostname = 'bcn-w10' THEN qde.pcpu ELSE 0 END) as w10,
    MAX(CASE WHEN qde.hostname = 'bcn-w11' THEN qde.pcpu ELSE 0 END) as w11,
    MAX(CASE WHEN qde.hostname = 'bcn-w12' THEN qde.pcpu ELSE 0 END) as w12,
    MAX(CASE WHEN qde.hostname = 'bcn-w13' THEN qde.pcpu ELSE 0 END) as w13,
    MAX(CASE WHEN qde.hostname = 'bcn-w14' THEN qde.pcpu ELSE 0 END) as w14,
    MAX(CASE WHEN qde.hostname = 'bcn-w15' THEN qde.pcpu ELSE 0 END) as w15,
    MAX(CASE WHEN qde.hostname = 'bcn-w16' THEN qde.pcpu ELSE 0 END) as w16
  FROM qde_mem_cpu_per_node as qde
  WHERE qde.begintime BETWEEN start_time AND end_time
  GROUP BY qde.timeslot;
END
$$ LANGUAGE PLPGSQL;

grant all on workload_run_info to hawq_cov;
grant all on  qde_mem_cpu_per_node to hawq_cov;
select * from f_get_host_cpu('tpch_row_200gpn_quicklz1_nopart');
select * from f_get_host_memory('tpch_row_200gpn_quicklz1_nopart_CONCURRENT') order by timeslot;


---------------------------------------------------------------------------------------------------------------
INSERT INTO hst.qd_info select * from hst.qd_info_history WHERE run_id in (116,117,118,119);
TRUNCATE TABLE hst.qd_info_history;

INSERT INTO hst.qd_mem_cpu select * from hst.qd_mem_cpu_history WHERE run_id in (116,117,118,119);
TRUNCATE TABLE hst.qd_mem_cpu_history;

INSERT INTO hst.qe_mem_cpu select * from hst.qe_mem_cpu_history WHERE run_id in (116,117,118,119);
TRUNCATE TABLE hst.qe_mem_cpu_history;





