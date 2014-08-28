select
 sum(l_extendedprice) / 7.0 as avg_yearly
from
 lineitem_TABLESUFFIX,
 part_TABLESUFFIX
where
 p_partkey = l_partkey
 and p_brand = 'Brand#54'
 and p_container = 'JUMBO CASE'
 and l_quantity < (
 select
 0.2 * avg(l_quantity)
 from
 lineitem_TABLESUFFIX
 where
 l_partkey = p_partkey
 );