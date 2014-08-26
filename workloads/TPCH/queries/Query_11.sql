-- start ignore
explain
select
 ps_partkey,
 sum(ps_supplycost * ps_availqty) as value
from
 TABLESUFFIX_partsupp,
 TABLESUFFIX_supplier,
 TABLESUFFIX_nation
where
 ps_suppkey = s_suppkey
 and s_nationkey = n_nationkey
 and n_name = 'ARGENTINA'
group by
 ps_partkey having
 sum(ps_supplycost * ps_availqty) > (
 select
 sum(ps_supplycost * ps_availqty) * 0.0000010000
 from
 TABLESUFFIX_partsupp,
 TABLESUFFIX_supplier,
 TABLESUFFIX_nation
 where
 ps_suppkey = s_suppkey
 and s_nationkey = n_nationkey
 and n_name = 'ARGENTINA'
 )
order by
 value desc;
-- end ignore


select
 ps_partkey,
 sum(ps_supplycost * ps_availqty) as value
from
 TABLESUFFIX_partsupp,
 TABLESUFFIX_supplier,
 TABLESUFFIX_nation
where
 ps_suppkey = s_suppkey
 and s_nationkey = n_nationkey
 and n_name = 'ARGENTINA'
group by
 ps_partkey having
 sum(ps_supplycost * ps_availqty) > (
 select
 sum(ps_supplycost * ps_availqty) * 0.0000010000
 from
 TABLESUFFIX_partsupp,
 TABLESUFFIX_supplier,
 TABLESUFFIX_nation
 where
 ps_suppkey = s_suppkey
 and s_nationkey = n_nationkey
 and n_name = 'ARGENTINA'
 )
order by
 value desc;
