INSERT INTO {{ sink('ch', 'orders_out') }}
select id, name
from {{ source('ch', 'orders_in') }}
order by id
