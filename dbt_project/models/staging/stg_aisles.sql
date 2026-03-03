/*
  stg_aisles.sql
  Reference table. 134 aisles.
*/

{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'aisles') }}
)

SELECT
    aisle_id::INT AS aisle_id,
    aisle AS aisle_name
FROM source