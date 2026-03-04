/*
  dim_users.sql
  User dimension. SCD TYPE 1.
  User attributes are mostly static, and when they do change (e.g. customer_segment),
  Grain: one row per user_id.
*/

{{ config(materialized='table', tags=['core','dimension']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }}
                                            AS user_key,
    user_id,
    total_orders,
    total_items_ordered,
    ROUND(avg_basket_size::NUMERIC, 2) AS avg_basket_size,
    ROUND(avg_days_between_orders::NUMERIC,1) AS avg_days_between_orders,
    most_common_order_day,
    most_common_order_hour,
    customer_segment,
    loyalty_tier,
    order_frequency_bucket,
    ROUND(reorder_rate::NUMERIC, 4) AS reorder_rate,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_average,
    CURRENT_TIMESTAMP AS updated_at

FROM {{ ref('mart_customer_360') }}