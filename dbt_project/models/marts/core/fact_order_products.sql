/*
  fact_order_products.sql
  LINE ITEM FACT TABLE. One row per (order, product).

  Most granular fact table — enables product-level analysis.
  fact_orders answers: "what happened at basket level?"
  fact_order_products answers: "what happened at product level?"

  Example queries enabled:
  - Which products are most reordered?
  - Which products appear together most often?
  - Which products are always added first to cart?
*/

{{
  config(
    materialized = 'incremental',
    unique_key   = 'order_product_id',
    tags         = ['core','fact']
  )
}}

WITH order_products AS (
    SELECT * FROM {{ ref('stg_order_products') }}
),

products AS (
    SELECT product_id, product_key, department_id
    FROM {{ ref('dim_products') }}
    WHERE is_current = TRUE
),

users AS (
    SELECT user_id, user_key FROM {{ ref('dim_users') }}
),

orders AS (
    SELECT order_id, user_id, order_day_of_week
    FROM {{ ref('stg_orders') }}
)

SELECT
    op.order_product_id,

    -- Foreign keys
    op.order_id,                            -- → fact_orders
    p.product_key,                          -- → dim_products
    u.user_key,                             -- → dim_users
    o.order_day_of_week AS date_key,

    -- Natural key kept for convenience
    op.product_id,

    -- Measures
    op.add_to_cart_order,
    op.is_reordered,
    op.is_early_cart_addition,

    CASE
        WHEN op.add_to_cart_order <= 5  THEN '1-5 Staples'
        WHEN op.add_to_cart_order <= 15 THEN '6-15 Regular'
        ELSE '16+ Browse'
    END AS cart_position_bucket,
    op.order_set

FROM order_products op
LEFT JOIN products p ON op.product_id = p.product_id
LEFT JOIN orders   o ON op.order_id   = o.order_id
LEFT JOIN users    u ON o.user_id     = u.user_id

{% if is_incremental() %}
WHERE op.order_id NOT IN (
    SELECT DISTINCT order_id FROM {{ this }}
)
{% endif %}