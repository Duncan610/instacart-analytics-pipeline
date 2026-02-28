/*
  safe_divide.sql
  ─────────────────────────────────────────────────────────────────────────
  Macro: Safely divide two numbers, returning NULL instead of divide-by-zero.

  Usage:
    {{ safe_divide('numerator_column', 'denominator_column') }}
    {{ safe_divide('revenue', 'orders', default=0) }}
*/

{% macro safe_divide(numerator, denominator, default='NULL') %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN {{ default }}
        ELSE {{ numerator }}::FLOAT / {{ denominator }}::FLOAT
    END
{% endmacro %}
