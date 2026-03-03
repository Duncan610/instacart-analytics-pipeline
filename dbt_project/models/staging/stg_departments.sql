/*
  stg_departments.sql
  Reference table. 21 departments.
*/

{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'departments') }}
)

SELECT
    department_id::INT  AS department_id,
    department AS department_name
FROM source