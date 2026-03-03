{{ config(materialized='view') }}

WITH prior AS (
    SELECT
        order_id::INT AS order_id,
        product_id::INT AS product_id,
        add_to_cart_order::INT AS add_to_cart_order,
        reordered::INT AS reordered,
        'prior' AS order_set
    FROM {{ source('raw', 'order_products_prior') }}
),

train AS (
    SELECT
        order_id::INT AS order_id,
        product_id::INT AS product_id,
        add_to_cart_order::INT  AS add_to_cart_order,
        reordered::INT AS reordered,
        'train' AS order_set
    FROM {{ source('raw', 'order_products_train') }}
),

unioned AS (
    SELECT * FROM prior
    UNION ALL
    SELECT * FROM train
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}
        AS order_product_id,
    order_id,
    product_id,
    add_to_cart_order,
    reordered::BOOLEAN AS is_reordered,
    order_set,
    CASE
        WHEN add_to_cart_order <= 5 THEN TRUE
        ELSE FALSE
    END AS is_early_cart_addition
FROM unioned