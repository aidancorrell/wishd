-- The model whose lineage we care about: two inputs, one output.
select o.order_id, o.customer_id, c.segment, o.order_total
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c on c.customer_id = o.customer_id
