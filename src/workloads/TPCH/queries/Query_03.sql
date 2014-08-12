-- start ignore
explain
select
 l_orderkey,
 sum(l_extendedprice * (1 - l_discount)) as revenue,
 o_orderdate,
 o_shippriority
from
 customerTABLESUFFIX,
 ordersTABLESUFFIX,
 lineitemTABLESUFFIX
where
 c_mktsegment = 'FURNITURE'
 and c_custkey = o_custkey
 and l_orderkey = o_orderkey
 and o_orderdate < date '1995-03-26'
 and l_shipdate > date '1995-03-26'
group by
 l_orderkey,
 o_orderdate,
 o_shippriority
order by
 revenue desc,
 o_orderdate
LIMIT 10;
-- end ignore

select
 l_orderkey,
 sum(l_extendedprice * (1 - l_discount)) as revenue,
 o_orderdate,
 o_shippriority
from
 customerTABLESUFFIX,
 ordersTABLESUFFIX,
 lineitemTABLESUFFIX
where
 c_mktsegment = 'FURNITURE'
 and c_custkey = o_custkey
 and l_orderkey = o_orderkey
 and o_orderdate < date '1995-03-26'
 and l_shipdate > date '1995-03-26'
group by
 l_orderkey,
 o_orderdate,
 o_shippriority
order by
 revenue desc,
 o_orderdate
LIMIT 10;
