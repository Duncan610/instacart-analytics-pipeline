/*
  dim_departments.sql
  Department dimension. 21 rows. SCD Type 1.
  Department names rarely change — overwrite is correct here.
*/

{{ config(materialized='table', tags=['core','dimension']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['department_id']) }}
                            AS department_key,
    department_id,
    department_name,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM {{ ref('stg_departments') }}