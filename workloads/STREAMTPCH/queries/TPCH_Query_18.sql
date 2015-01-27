select
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice,
 sum(l_quantity) as total_quantity
from
 customer_TABLESUFFIX,
 orders_TABLESUFFIX,
 lineitem_TABLESUFFIX
where
 o_orderkey in (
 select
 l_orderkey
 from
 lineitem_TABLESUFFIX
 group by
 l_orderkey having
 sum(l_quantity) > 314
 )
 and c_custkey = o_custkey
 and o_orderkey = l_orderkey
group by
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice
order by
 o_totalprice desc,
 total_quantity desc,
 o_orderdate,
 c_name,
 c_custkey,
 o_orderkey
LIMIT 100;