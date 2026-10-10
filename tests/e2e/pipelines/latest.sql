INSERT INTO {{ sink('ch', 'orders_latest') }}
select id, name
from {{ ref('relay') }}
