#!/usr/bin/env bash
set -e
echo "Bootstrap RAW layer in Snowflake..."
# Edit account/credentials or rely on ~/.snowsql config
snowsql -f ddl/create_raw.sql
echo "Done!"
