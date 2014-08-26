-- start ignore
explain
select
 TABLESUFFIX_nation,
 o_year,
 sum(amount) as sum_profit
from
 (
 select
 n_name as TABLESUFFIX_nation,
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 from
 TABLESUFFIX_part,
 TABLESUFFIX_supplier,
 TABLESUFFIX_lineitem,
 TABLESUFFIX_partsupp,
 TABLESUFFIX_orders,
 TABLESUFFIX_nation
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
 TABLESUFFIX_nation,
 o_year
order by
 TABLESUFFIX_nation,
 o_year desc;
-- end ignore

select
 TABLESUFFIX_nation,
 o_year,
 sum(amount) as sum_profit
from
 (
 select
 n_name as TABLESUFFIX_nation,
 extract(year from o_orderdate) as o_year,
 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 from
 TABLESUFFIX_part,
 TABLESUFFIX_supplier,
 TABLESUFFIX_lineitem,
 TABLESUFFIX_partsupp,
 TABLESUFFIX_orders,
 TABLESUFFIX_nation
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
 TABLESUFFIX_nation,
 o_year
order by
 TABLESUFFIX_nation,
 o_year desc;
