/*
  Custom singular test.
  Returns rows where reorder_rate is invalid.
  dbt passes this test when ZERO rows are returned.
*/

SELECT
    user_id,
    reorder_rate
FROM {{ ref('mart_customer_360') }}
WHERE reorder_rate < 0
   OR reorder_rate > 1