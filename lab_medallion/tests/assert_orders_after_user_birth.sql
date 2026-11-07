{{ config(severity='warn') }}

select o.order_id, o.ordered_at, u.user_id, u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u using (user_id)
where o.ordered_at < u.birthdate
