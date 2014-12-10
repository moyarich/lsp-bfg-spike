DROP TABLE IF EXISTS moni.qe_mem_cpu_per_seg;
CREATE TABLE moni.qe_mem_cpu_per_seg AS 
SELECT time_point, con_id, seg_id, status, SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu 
FROM moni.qe_mem_cpu GROUP BY time_point, con_id, seg_id, status DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS moni.qe_mem_cpu_per_query;
CREATE TABLE moni.qe_mem_cpu_per_query AS 
SELECT time_point, con_id, status, SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu 
FROM moni.qe_mem_cpu GROUP BY time_point, con_id, status DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS moni.qe_mem_cpu_per_node;
CREATE TABLE moni.qe_mem_cpu_per_node AS 
SELECT time_point, SUM(rss) AS rss, SUM(pmem) AS pmem, SUM(pcpu) AS pcpu 
FROM moni.qe_mem_cpu GROUP BY time_point DISTRIBUTED RANDOMLY;