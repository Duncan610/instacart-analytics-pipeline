/*
  fact_orders.sql
  CENTRAL FACT TABLE. One row per order.
  Contains foreign keys to all dimensions +
  measurable metrics about each order.
*/

{{
  config(
    materialized    = 'incremental',
    unique_key      = 'order_id',
    on_schema_change= 'append_new_columns',
    tags            = ['core','fact']
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

line_items AS (
    SELECT
        order_id,
        COUNT(product_id)                               AS total_items,
        COUNT(DISTINCT product_id)                      AS unique_items,
        SUM(CASE WHEN is_reordered THEN 1 ELSE 0 END)  AS reordered_items,
        {{ safe_divide(
            'SUM(CASE WHEN is_reordered THEN 1 ELSE 0 END)',
            'COUNT(product_id)'
        ) }}                                            AS order_reorder_rate,
        AVG(add_to_cart_order)                          AS avg_cart_position,
        MIN(add_to_cart_order)                          AS first_item_cart_pos,
        MAX(add_to_cart_order)                          AS last_item_cart_pos,
        SUM(CASE WHEN is_early_cart_addition
            THEN 1 ELSE 0 END)                          AS early_cart_items
    FROM {{ ref('stg_order_products') }}
    GROUP BY order_id
),

users AS (
    SELECT user_id, user_key FROM {{ ref('dim_users') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}
                                            AS order_key,
    o.order_id,

    -- Foreign keys to dimensions
    u.user_key,
    o.order_day_of_week                     AS date_key,

    -- Descriptive attributes
    o.order_sequence_number,
    o.order_day_of_week,
    o.order_day_name,
    o.order_hour_of_day,
    o.order_time_of_day,
    o.is_first_order,

    -- Measures
    o.days_since_prior_order,
    COALESCE(li.total_items,    0) AS total_items,
    COALESCE(li.unique_items,   0) AS unique_items,
    COALESCE(li.reordered_items,0) AS reordered_items,
    COALESCE(ROUND(li.order_reorder_rate::NUMERIC, 4), 0) AS order_reorder_rate,
    ROUND(li.avg_cart_position::NUMERIC,  2)    AS avg_cart_position,
    li.first_item_cart_pos,
    li.last_item_cart_pos,
    COALESCE(li.early_cart_items, 0)            AS early_cart_items,
    o.loaded_at

FROM orders    o
LEFT JOIN users      u  ON o.user_id   = u.user_id
LEFT JOIN line_items li ON o.order_id  = li.order_id
