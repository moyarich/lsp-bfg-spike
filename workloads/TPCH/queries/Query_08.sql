-- start ignore
explain
select
 o_year,
 sum(case
 when TABLESUFFIX_nation = 'ETHIOPIA' then volume
 else 0
 end) / sum(volume) as mkt_share
from
 (
 select
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) as volume,
 n2.n_name as TABLESUFFIX_nation
 from
 TABLESUFFIX_part,
 TABLESUFFIX_supplier,
 TABLESUFFIX_lineitem,
 TABLESUFFIX_orders,
 TABLESUFFIX_customer,
 TABLESUFFIX_nation n1,
 TABLESUFFIX_nation n2,
 TABLESUFFIX_region
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
-- end ignore


select
 o_year,
 sum(case
 when TABLESUFFIX_nation = 'ETHIOPIA' then volume
 else 0
 end) / sum(volume) as mkt_share
from
 (
 select
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) as volume,
 n2.n_name as TABLESUFFIX_nation
 from
 TABLESUFFIX_part,
 TABLESUFFIX_supplier,
 TABLESUFFIX_lineitem,
 TABLESUFFIX_orders,
 TABLESUFFIX_customer,
 TABLESUFFIX_nation n1,
 TABLESUFFIX_nation n2,
 TABLESUFFIX_region
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
