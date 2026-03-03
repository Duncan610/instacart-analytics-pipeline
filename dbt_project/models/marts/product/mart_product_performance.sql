/*
  mart_product_performance.sql
  One row per product. Reorder rates, cart position,
  and popularity rankings by department.
*/

{{
  config(
    materialized = 'table',
    tags         = ['product', 'daily']
  )
}}

WITH order_products AS (
    SELECT * FROM {{ ref('stg_order_products') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

metrics AS (
    SELECT
        product_id,
        COUNT(*)                                        AS times_ordered,
        COUNT(DISTINCT order_id)                        AS unique_orders,
        SUM(CASE WHEN is_reordered THEN 1 ELSE 0 END)  AS times_reordered,

        {{ safe_divide(
            'SUM(CASE WHEN is_reordered THEN 1 ELSE 0 END)',
            'COUNT(*)'
        ) }}                                            AS reorder_rate,

        AVG(add_to_cart_order)                          AS avg_cart_position,
        MIN(add_to_cart_order)                          AS min_cart_position,

        {{ safe_divide(
            'SUM(CASE WHEN is_early_cart_addition THEN 1 ELSE 0 END)',
            'COUNT(*)'
        ) }}                                            AS pct_early_cart

    FROM order_products
    GROUP BY product_id
),

ranked AS (
    SELECT
        m.*,
        p.product_name,
        p.aisle_id,
        p.aisle_name,
        p.department_id,
        p.department_name,

        RANK() OVER (
            PARTITION BY p.department_id
            ORDER BY m.reorder_rate DESC
        )                                               AS dept_reorder_rank,

        RANK() OVER (
            ORDER BY m.times_ordered DESC
        )                                               AS overall_popularity_rank

    FROM metrics  m
    JOIN products p ON m.product_id = p.product_id
)

SELECT
    product_id,
    product_name,
    aisle_id,
    aisle_name,
    department_id,
    department_name,
    times_ordered,
    unique_orders,
    times_reordered,
    ROUND(reorder_rate::NUMERIC,    4)  AS reorder_rate,
    ROUND(avg_cart_position::NUMERIC,2) AS avg_cart_position,
    min_cart_position,
    ROUND(pct_early_cart::NUMERIC,  4)  AS pct_early_cart,
    dept_reorder_rank,
    overall_popularity_rank,

    CASE
        WHEN reorder_rate >= 0.7 THEN 'High Reorder'
        WHEN reorder_rate >= 0.4 THEN 'Medium Reorder'
        ELSE 'Low Reorder'
    END                                 AS reorder_tier,

    CASE
        WHEN overall_popularity_rank <= 100  THEN 'Top 100'
        WHEN overall_popularity_rank <= 500  THEN 'Top 500'
        WHEN overall_popularity_rank <= 1000 THEN 'Top 1000'
        ELSE 'Long Tail'
    END                                 AS popularity_tier

FROM ranked