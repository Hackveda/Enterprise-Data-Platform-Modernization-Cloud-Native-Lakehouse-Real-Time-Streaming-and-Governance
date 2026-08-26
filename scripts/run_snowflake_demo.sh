#!/usr/bin/env bash
set -euo pipefail

: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT}"
: "${SNOWFLAKE_USER:?Set SNOWFLAKE_USER}"
: "${SNOWFLAKE_PASSWORD:?Set SNOWFLAKE_PASSWORD}"

export SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE:-PLATFORM_WH}
export SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE:-ENTERPRISE_PLATFORM}
export SNOWFLAKE_ROLE=${SNOWFLAKE_ROLE:-PLATFORM_ROLE}
export DATABASE_URL=${DATABASE_URL:-postgresql://platform:platform@localhost:5432/commerce}

printf '\n[1/5] Extract PostgreSQL source to local raw Parquet\n'
python batch/extract_postgres.py

printf '\n[2/5] Load raw Parquet into Snowflake RAW schema\n'
python scripts/load_snowflake.py

printf '\n[3/5] Validate dbt connection\n'
dbt debug --project-dir warehouse/dbt_snowflake --profiles-dir warehouse/dbt_snowflake

printf '\n[4/5] Build Snowflake STAGING and MARTS layers\n'
dbt run --project-dir warehouse/dbt_snowflake --profiles-dir warehouse/dbt_snowflake

dbt test --project-dir warehouse/dbt_snowflake --profiles-dir warehouse/dbt_snowflake

printf '\n[5/5] Verify row counts and mart KPIs\n'
python scripts/verify_snowflake.py

printf '\nSnowflake demo completed successfully.\n'
