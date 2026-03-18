FROM apache/airflow:2.9.3-python3.11

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.8.7 \
    dbt-snowflake==1.8.4 \
    dbt-postgres==1.8.2
