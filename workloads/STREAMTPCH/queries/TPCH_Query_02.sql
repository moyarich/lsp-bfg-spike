select
 s_acctbal,
 s_name,
 n_name,
 p_partkey,
 p_mfgr,
 s_address,
 s_phone,
 s_comment
from
 part_TABLESUFFIX,
 supplier_TABLESUFFIX,
 partsupp_TABLESUFFIX,
 nation_TABLESUFFIX,
 region_TABLESUFFIX
where
 p_partkey = ps_partkey
 and s_suppkey = ps_suppkey
 and p_size = 50
 and p_type like '%COPPER'
 and s_nationkey = n_nationkey
 and n_regionkey = r_regionkey
 and r_name = 'MIDDLE EAST'
 and ps_supplycost = (
 select
 min(ps_supplycost)
 from
 partsupp_TABLESUFFIX,
 supplier_TABLESUFFIX,
 nation_TABLESUFFIX,
 region_TABLESUFFIX
 where
 p_partkey = ps_partkey
 and s_suppkey = ps_suppkey
 and s_nationkey = n_nationkey
 and n_regionkey = r_regionkey
 and r_name = 'MIDDLE EAST'
 )
order by
 s_acctbal desc,
 n_name,
 s_name,
 p_partkey
LIMIT 100;