/*
  dim_dates.sql
  Calendar dimension. One row per date.
  Generated using dbt_utils.date_spine — no gaps ever.
  All fact tables join here for time-based slicing.
  SCD: Not applicable. Dates never change.
*/

{{ config(materialized='table', tags=['core','dimension']) }}

WITH date_spine AS (
    {{
      dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2019-01-01' as date)",
        end_date   = "cast('2019-12-31' as date)"
      )
    }}
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_key,
    date_day AS full_date,
    EXTRACT(year    FROM date_day)::INT AS year,
    EXTRACT(quarter FROM date_day)::INT AS quarter_number,
    'Q' || EXTRACT(quarter FROM date_day) AS quarter_name,
    EXTRACT(month   FROM date_day)::INT AS month_number,
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Mon') AS month_short_name,
    EXTRACT(week FROM date_day)::INT AS week_of_year,
    EXTRACT(day FROM date_day)::INT AS day_of_month,
    EXTRACT(doy FROM date_day)::INT AS day_of_year,
    EXTRACT(dow FROM date_day)::INT AS day_of_week_number,
    TO_CHAR(date_day, 'Day')  AS day_name,
    TO_CHAR(date_day, 'Dy') AS day_short_name,

    CASE
        WHEN EXTRACT(dow FROM date_day) IN (0,6)
        THEN TRUE ELSE FALSE
    END AS is_weekend,

    CASE
        WHEN EXTRACT(dow FROM date_day) IN (0,6)
        THEN FALSE ELSE TRUE
    END AS is_weekday,

    CASE
        WHEN EXTRACT(month FROM date_day) IN (12,1,2)  THEN 'Winter'
        WHEN EXTRACT(month FROM date_day) IN (3,4,5)   THEN 'Spring'
        WHEN EXTRACT(month FROM date_day) IN (6,7,8)   THEN 'Summer'
        WHEN EXTRACT(month FROM date_day) IN (9,10,11) THEN 'Fall'
    END AS season

FROM date_spine
ORDER BY full_date