/*
  int_customer_order_history.sql
  Aggregates all order data to customer level.
  This is ephemeral — no table created, compiled as
  a CTE inside downstream mart models.
  Grain: one row per user_id.
*/

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_products AS (
    SELECT * FROM {{ ref('stg_order_products') }}
),

order_stats AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id) AS total_orders,
        AVG(days_since_prior_order) AS avg_days_between_orders,
        MIN(days_since_prior_order) AS min_days_between_orders,
        MAX(days_since_prior_order) AS max_days_between_orders,
        MODE() WITHIN GROUP (ORDER BY order_day_name) AS most_common_order_day,
        MODE() WITHIN GROUP (ORDER BY order_hour_of_day) AS most_common_order_hour
    FROM orders
    WHERE NOT is_first_order
    GROUP BY user_id
),

product_stats AS (
    SELECT
        o.user_id,
        COUNT(op.product_id) AS total_items_ordered,

        {{ safe_divide(
            'COUNT(op.product_id)',
            'COUNT(DISTINCT o.order_id)'
        ) }} AS avg_basket_size,

        SUM(CASE WHEN op.is_reordered THEN 1 ELSE 0 END) AS total_reordered_items,

        {{ safe_divide(
            'SUM(CASE WHEN op.is_reordered THEN 1 ELSE 0 END)',
            'COUNT(op.product_id)'
        ) }} AS reorder_rate,

        {{ safe_divide(
            'SUM(CASE WHEN op.is_early_cart_addition THEN 1 ELSE 0 END)',
            'COUNT(op.product_id)'
        ) }} AS pct_items_early_cart

    FROM orders o
    JOIN order_products op ON o.order_id = op.order_id
    GROUP BY o.user_id
)

SELECT
    os.user_id,
    os.total_orders,
    os.avg_days_between_orders,
    os.min_days_between_orders,
    os.max_days_between_orders,
    os.most_common_order_day,
    os.most_common_order_hour,
    ps.total_items_ordered,
    ps.avg_basket_size,
    ps.total_reordered_items,
    ps.reorder_rate,
    ps.pct_items_early_cart

FROM order_stats   os
LEFT JOIN product_stats ps ON os.user_id = ps.user_id