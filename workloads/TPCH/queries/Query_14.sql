-- start ignore
explain
select
 100.00 * sum(case
 when p_type like 'PROMO%'
 then l_extendedprice * (1 - l_discount)
 else 0
 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
 TABLESUFFIX_lineitem,
 TABLESUFFIX_part
where
 l_partkey = p_partkey
 and l_shipdate >= date '1997-04-01'
 and l_shipdate < date '1997-04-01' + interval '1 month';
-- end ignore

select
 100.00 * sum(case
 when p_type like 'PROMO%'
 then l_extendedprice * (1 - l_discount)
 else 0
 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
 TABLESUFFIX_lineitem,
 TABLESUFFIX_part
where
 l_partkey = p_partkey
 and l_shipdate >= date '1997-04-01'
 and l_shipdate < date '1997-04-01' + interval '1 month';
