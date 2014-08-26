-- start ignore
explain
select
 sum(l_extendedprice) / 7.0 as avg_yearly
from
 TABLESUFFIX_lineitem,
 TABLESUFFIX_part
where
 p_partkey = l_partkey
 and p_brand = 'Brand#54'
 and p_container = 'JUMBO CASE'
 and l_quantity < (
 select
 0.2 * avg(l_quantity)
 from
 TABLESUFFIX_lineitem
 where
 l_partkey = p_partkey
 );
-- end ignore

select
 sum(l_extendedprice) / 7.0 as avg_yearly
from
 TABLESUFFIX_lineitem,
 TABLESUFFIX_part
where
 p_partkey = l_partkey
 and p_brand = 'Brand#54'
 and p_container = 'JUMBO CASE'
 and l_quantity < (
 select
 0.2 * avg(l_quantity)
 from
 TABLESUFFIX_lineitem
 where
 l_partkey = p_partkey
 );
