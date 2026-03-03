/*
  stg_orders.sql
  ─────────────────────────────────────────────────────────────────────────
  Staging model for orders. Responsibilities:
    1. Rename columns to snake_case business-friendly names
    2. Cast data types explicitly
    3. Derive simple boolean flags (no complex business logic here)
    4. Handle known nulls (days_since_prior_order is null for first orders)

  Grain: One row per order
  Source: raw.orders
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

renamed AS (
    SELECT
        -- Primary key
        order_id::INT AS order_id,

        -- Foreign keys
        user_id::INT AS user_id,

        -- Order attributes
        order_number::INT AS order_sequence_number,  -- renamed for clarity
        order_dow::INT AS order_day_of_week,       -- 0=Sunday, 6=Saturday
        order_hour_of_day::INT AS order_hour_of_day,

        -- name days
        CASE order_dow::INT
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS order_day_name,

        -- Time bucket
        CASE
            WHEN order_hour_of_day::INT BETWEEN 5 AND 11  THEN 'Morning'
            WHEN order_hour_of_day::INT BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN order_hour_of_day::INT BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS order_time_of_day,

        -- Null handling
        days_since_prior_order::FLOAT AS days_since_prior_order,
        CASE
            WHEN days_since_prior_order IS NULL THEN TRUE
            ELSE FALSE
        END AS is_first_order,

        
        _loaded_at AS loaded_at

    FROM source
)

SELECT * FROM renamed
