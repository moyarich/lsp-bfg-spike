-----
--This table is to aggerate all process for each segment 
DROP TABLE IF EXISTS moni.qe_mem_cpu_per_seg_con;
CREATE TABLE moni.qe_mem_cpu_per_seg_con AS
SELECT hostname, timeslot, min(real_time) as begintime,
con_id, seg_id,
SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
FROM moni.qe_mem_cpu
GROUP BY hostname, timeslot, con_id, seg_id DISTRIBUTED RANDOMLY;

------ for each connection, get total cpu and memory for per node
DROP VIEW IF EXISTS moni.qe_mem_cpu_per_node_con;
CREATE OR REPLACE VIEW  moni.qe_mem_cpu_per_node_con AS
SELECT hostname, timeslot, begintime, con_id,
SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
FROM moni.qe_mem_cpu_per_seg_con
GROUP BY hostname,timeslot, begintime, con_id;

------ get total cpu and memory for evyer node
DROP VIEW IF EXISTS moni.qe_mem_cpu_per_node;
CREATE OR REPLACE VIEW  moni.qe_mem_cpu_per_node AS
SELECT hostname, timeslot, begintime,
SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
FROM moni.qe_mem_cpu_per_seg_con
GROUP BY hostname,timeslot, begintime
ORDER BY hostname, timeslot;


------ Get every 5 minute total memory and cpu for every  node 
SELECT hostname, timeslot/300 as tm,  max(rss),max(pcpu)
from moni.qe_mem_cpu_per_node
group by hostname, timeslot/300
order by hostname,tm;

---------For each get total memory/cpu for each node
CREATE TABLE moni.qd_qe_query_mem_cpu_per_query AS
SELECT
        qd_stat.con_id,
        qd_stat.query_start_time,
        qd_stat.query_end_time,
        qe.timeslot as timeslot,
        qe.hostname,
        qe.rss as qerss,
        qe.pmem as qepmem,
        qe.pcpu as qecpu,
        qd.hostname,
        qd.rss as qdrss,
        qd.pmem as qdpmem,
        qd.pcpu as qdcpu
FROM  qd_info as qd_stat,
      (SELECT hostname, timeslot, min(real_time) as begintime, con_id,
                     SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu
      FROM qd_mem_cpu
      GROUP BY hostname, timeslot, con_id) as qd,
      qe_mem_cpu_per_seg_con as qe
WHERE qe.con_id = qd_stat.con_id AND qe.begintime >= qd_stat.query_start_time
       AND qe.begintime <= qd_stat.query_end_time
      AND qd.con_id = qd_stat.con_id and qd.begintime >= qd_stat.query_start_time
      AND qd.begintime <= qd_stat.query_end_time
DISTRIBUTED RANDOMLY;
