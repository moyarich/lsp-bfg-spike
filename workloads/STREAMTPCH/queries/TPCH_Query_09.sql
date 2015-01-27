select
 nation_TABLESUFFIX,
 o_year,
 sum(amount) as sum_profit
from
 (
 select
 n_name as nation_TABLESUFFIX,
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 from
 part_TABLESUFFIX,
 supplier_TABLESUFFIX,
 lineitem_TABLESUFFIX,
 partsupp_TABLESUFFIX,
 orders_TABLESUFFIX,
 nation_TABLESUFFIX
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
 nation_TABLESUFFIX,
 o_year
order by
 nation_TABLESUFFIX,
 o_year desc;