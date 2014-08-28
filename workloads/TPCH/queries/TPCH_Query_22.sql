select
 cntrycode,
 count(*) as numcust,
 sum(c_acctbal) as totacctbal
from
 (
 select
 substr(c_phone, 1, 2) as cntrycode,
 c_acctbal
 from
 customer_TABLESUFFIX
 where
 substr(c_phone, 1, 2) in
 ('15', '29', '27', '17', '31', '22', '19')
 and c_acctbal > (
 select
 avg(c_acctbal)
 from
 customer_TABLESUFFIX
 where
 c_acctbal > 0.00
 and substr(c_phone, 1, 2) in
 ('15', '29', '27', '17', '31', '22', '19')
 )
 and not exists (
 select
 *
 from
 orders_TABLESUFFIX
 where
 o_custkey = c_custkey
 )
 ) as vip
group by
 cntrycode
order by
 cntrycode;