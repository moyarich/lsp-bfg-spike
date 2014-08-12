-- start ignore
explain
select
 s_name,
 s_address
from
 supplier,
 nation
where
 s_suppkey in (
 select
 distinct (ps_suppkey)
 from
 partsupp,
 part
 where
 ps_partkey=p_partkey
 and p_name like 'blush%'
 and ps_availqty > (
 select
 0.5 * sum(l_quantity)
 from
 lineitem
 where
 l_partkey = ps_partkey
 and l_suppkey = ps_suppkey
 and l_shipdate >= '1997-01-01'
 and l_shipdate < date '1997-01-01' + interval '1 year'
 )
 )
 and s_nationkey = n_nationkey
 and n_name = 'SAUDI ARABIA'
order by
 s_name;
-- end ignore

select
 s_name,
 s_address
from
 supplier,
 nation
where
 s_suppkey in (
 select
 distinct (ps_suppkey)
 from
 partsupp,
 part
 where
 ps_partkey=p_partkey
 and p_name like 'blush%'
 and ps_availqty > (
 select
 0.5 * sum(l_quantity)
 from
 lineitem
 where
 l_partkey = ps_partkey
 and l_suppkey = ps_suppkey
 and l_shipdate >= '1997-01-01'
 and l_shipdate < date '1997-01-01' + interval '1 year'
 )
 )
 and s_nationkey = n_nationkey
 and n_name = 'SAUDI ARABIA'
order by
 s_name;
