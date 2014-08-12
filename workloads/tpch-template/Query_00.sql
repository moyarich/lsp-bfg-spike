drop view if exists revenue;

create view revenue (supplier_no, total_revenue) as
 select
 l_suppkey,
 sum(l_extendedprice * (1 - l_discount))
 from
 lineitem
 where
 l_shipdate >= date '1997-04-01'
 and l_shipdate < date '1997-04-01' + interval '90 days'
 group by
 l_suppkey;
