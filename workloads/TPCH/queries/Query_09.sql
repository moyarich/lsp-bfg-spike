-- start ignore
explain
select
 nationTABLESUFFIX,
 o_year,
 sum(amount) as sum_profit
from
 (
 select
 n_name as nationTABLESUFFIX,
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 from
 partTABLESUFFIX,
 supplierTABLESUFFIX,
 lineitemTABLESUFFIX,
 partsuppTABLESUFFIX,
 ordersTABLESUFFIX,
 nationTABLESUFFIX
 where
 s_suppkey = l_suppkey
 and ps_suppkey = l_suppkey
 and ps_partkey = l_partkey
 and p_partkey = l_partkey
 and o_orderkey = l_orderkey
 and s_nationkey = n_nationkey
 and p_name like '%aquamarine%'
 ) as profit
group by
 nationTABLESUFFIX,
 o_year
order by
 nationTABLESUFFIX,
 o_year desc;
-- end ignore

select
 nationTABLESUFFIX,
 o_year,
 sum(amount) as sum_profit
from
 (
 select
 n_name as nationTABLESUFFIX,
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 from
 partTABLESUFFIX,
 supplierTABLESUFFIX,
 lineitemTABLESUFFIX,
 partsuppTABLESUFFIX,
 ordersTABLESUFFIX,
 nationTABLESUFFIX
 where
 s_suppkey = l_suppkey
 and ps_suppkey = l_suppkey
 and ps_partkey = l_partkey
 and p_partkey = l_partkey
 and o_orderkey = l_orderkey
 and s_nationkey = n_nationkey
 and p_name like '%aquamarine%'
 ) as profit
group by
 nationTABLESUFFIX,
 o_year
order by
 nationTABLESUFFIX,
 o_year desc;
