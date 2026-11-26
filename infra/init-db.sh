#!/bin/sh
(
set -eu

psql --username "$POSTGRES_USER" --dbname postgres -v ON_ERROR_STOP=1 \
    -v airflow_password="$AIRFLOW_DB_PASSWORD" -v market_password="$PIPELINE_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
CREATE ROLE market LOGIN PASSWORD :'market_password';
CREATE DATABASE market OWNER market;
SQL

psql --username "$POSTGRES_USER" --dbname market -v ON_ERROR_STOP=1 --single-transaction -f - <<'SQL'
SET ROLE market;
\i /schema/schema.sql
SQL
)
