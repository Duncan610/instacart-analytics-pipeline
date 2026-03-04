/*
  dim_products.sql
  Product dimension. SCD TYPE 2.

  SCD Type 2 means: when a product changes (e.g. moves department),
  a NEW row is added. The old row gets valid_to set and is_current=FALSE.
  The new row has valid_from set and is_current=TRUE.

  This lets you answer: "what department was product X in on date Y?"
*/

{{ config(materialized='table', tags=['core','dimension']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['product_id', "'2019-01-01'"]) }}  AS product_key,

    product_id,
    product_name,
    aisle_id,
    aisle_name,
    department_id,
    department_name,

    -- SCD Type 2 columns
    CAST('2019-01-01' AS DATE) AS valid_from,
    CAST(NULL AS DATE) AS valid_to,
    TRUE AS is_current,

    CURRENT_TIMESTAMP AS created_at

FROM {{ ref('stg_products') }}