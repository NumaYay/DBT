WITH successful_payments AS (
select order_id,amount 
from {{ ref('stg_stripe__payments') }}
where status = "success";
),

completed_orders AS (
SELECT o.first_name, o.last_name, o.user_id, c.status as customer_status
from {{ ref('stg_jaffle_shop__orders') }} as o
INNER JOIN {{ ref('stg_jaffle_shop__customers') }} as c
ON o.user_id = c.id
where o.status = "completed"
),

customer_payments as (
SELECT
    co.first_name,
    co.last_name,
    co.customer_status,
    SUM(sp.amount) AS total_amount_paid
FROM completed_orders AS co
INNER JOIN successful_payments AS sp
ON co.order_id = sp.order_id
GROUP BY co.user_id, co.first_name, co.last_name, co.customer_status
),
output_table AS (
    SELECT
        cp.user_id,
        cp.first_name,
        cp.last_name,
        cp.customer_status,
        cp.total_amount_paid,
        RANK() OVER (ORDER BY cp.total_amount_paid DESC) AS rank
    FROM customer_payments AS cp
)

SELECT *
FROM ranked_customers
ORDER BY rank;