select
 o_year,
 sum(case
 when nation_TABLESUFFIX = 'ETHIOPIA' then volume
 else 0
 end) / sum(volume) as mkt_share
from
 (
 select
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) as volume,
 n2.n_name as nation_TABLESUFFIX
 from
 part_TABLESUFFIX,
 supplier_TABLESUFFIX,
 lineitem_TABLESUFFIX,
 orders_TABLESUFFIX,
 customer_TABLESUFFIX,
 nation_TABLESUFFIX n1,
 nation_TABLESUFFIX n2,
 region_TABLESUFFIX
 where
 p_partkey = l_partkey
 and s_suppkey = l_suppkey
 and l_orderkey = o_orderkey
 and o_custkey = c_custkey
 and c_nationkey = n1.n_nationkey
 and n1.n_regionkey = r_regionkey
 and r_name = 'AFRICA'
 and s_nationkey = n2.n_nationkey
 and o_orderdate between date '1995-01-01' and date '1996-12-31'
 and p_type = 'STANDARD ANODIZED COPPER'
 ) as all_nations
group by
 o_year
order by
 o_year;