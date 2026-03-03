/*
  mart_customer_360.sql
  One row per customer. RFM segmentation using NTILE(4).
  Powers marketing campaigns, churn analysis, loyalty programs.
*/

{{
  config(
    materialized = 'table',
    tags = ['marketing', 'daily']
  )
}}

WITH history AS (
    SELECT * FROM {{ ref('int_customer_order_history') }}
),

rfm AS (
    SELECT
        user_id,
        NTILE(4) OVER (
            ORDER BY avg_days_between_orders DESC NULLS LAST
        ) AS recency_score,
        NTILE(4) OVER (
            ORDER BY total_orders ASC
        ) AS frequency_score,
        NTILE(4) OVER (
            ORDER BY total_items_ordered ASC
        ) AS monetary_score
    FROM history
),

segments AS (
    SELECT
        user_id,
        recency_score,
        frequency_score,
        monetary_score,
        ROUND(
            (recency_score + frequency_score + monetary_score)::NUMERIC / 3,
            2
        ) AS rfm_average,

        CASE
            WHEN recency_score >= 3
             AND frequency_score >= 3  THEN 'Champion'
            WHEN recency_score >= 3
             AND frequency_score >= 2  THEN 'Loyal Customer'
            WHEN recency_score >= 3
             AND frequency_score <  2  THEN 'New Customer'
            WHEN recency_score  = 2
             AND frequency_score >= 3  THEN 'At Risk'
            WHEN recency_score  = 2
             AND frequency_score >= 2  THEN 'Needs Attention'
            WHEN recency_score <  2
             AND frequency_score >= 3  THEN 'Cant Lose Them'
            WHEN recency_score <  2
             AND frequency_score <  2  THEN 'Hibernating'
            ELSE 'Unknown'
        END AS customer_segment

    FROM rfm
)

SELECT
    h.user_id,
    h.total_orders,
    h.avg_days_between_orders,
    h.min_days_between_orders,
    h.max_days_between_orders,
    h.most_common_order_day,
    h.most_common_order_hour,
    h.total_items_ordered,
    ROUND(h.avg_basket_size::NUMERIC, 2)  AS avg_basket_size,
    h.total_reordered_items,
    ROUND(h.reorder_rate::NUMERIC, 4)  AS reorder_rate,
    ROUND(h.pct_items_early_cart::NUMERIC,4) AS pct_items_early_cart,
    s.recency_score,
    s.frequency_score,
    s.monetary_score,
    s.rfm_average,
    s.customer_segment,

    CASE
        WHEN h.avg_days_between_orders <= 7  THEN 'Weekly'
        WHEN h.avg_days_between_orders <= 14 THEN 'Bi-Weekly'
        WHEN h.avg_days_between_orders <= 30 THEN 'Monthly'
        ELSE 'Infrequent'
    END AS order_frequency_bucket,

    CASE
        WHEN h.reorder_rate >= 0.7 THEN 'High Loyalty'
        WHEN h.reorder_rate >= 0.4 THEN 'Medium Loyalty'
        ELSE 'Low Loyalty'
    END AS loyalty_tier

FROM history   h
LEFT JOIN segments s ON h.user_id = s.user_id