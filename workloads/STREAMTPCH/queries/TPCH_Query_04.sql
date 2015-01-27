select
 o_orderpriority,
 count(*) as order_count
from
 orders_TABLESUFFIX
where
 o_orderdate >= date '1997-02-01'
 and o_orderdate < date '1997-02-01' + interval '3 month'
 and exists (
 select
 *
 from
 lineitem_TABLESUFFIX
 where
 l_orderkey = o_orderkey
 and l_commitdate < l_receiptdate
 )
group by
 o_orderpriority
order by
 o_orderpriority;