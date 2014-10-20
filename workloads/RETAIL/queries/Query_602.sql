-- Select all customers whose last order was in NM. non-colocated join
select customer_id
,      state_code
from  (select li.customer_id
       ,      addr.state_code
       ,      row_number() over (partition by li.customer_id order by li.order_datetime desc) as rn
       from   retail_demo.order_lineitems li
       left outer join retail_demo.customer_Addresses_dim addr
         on addr.customer_id = li.customer_id
        and li.order_datetime between addr.valid_from_timestamp and addr.valid_to_timestamp
      ) a
where  rn = 1
and    a.state_code = 'NM'
;
