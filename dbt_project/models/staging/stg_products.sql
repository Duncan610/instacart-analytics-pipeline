/*
  stg_products.sql
  Joins products with aisles and departments so every
  product row already has human-readable names.
  Grain: one row per product.
*/

WITH products AS (
    SELECT * FROM {{ source('raw', 'products') }}
),
aisles AS (
    SELECT * FROM {{ source('raw', 'aisles') }}
),
departments AS (
    SELECT * FROM {{ source('raw', 'departments') }}
),

final AS (
    SELECT
        p.product_id::INT       AS product_id,
        p.product_name          AS product_name,
        p.aisle_id::INT         AS aisle_id,
        a.aisle                 AS aisle_name,
        p.department_id::INT    AS department_id,
        d.department            AS department_name

    FROM products      p
    LEFT JOIN aisles      a ON p.aisle_id      = a.aisle_id
    LEFT JOIN departments d ON p.department_id = d.department_id
)

SELECT * FROM final