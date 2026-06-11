#!/usr/bin/env sh

# ============================================================
# create_olap_role_and_database.sh
#
# Purpose:
#   Create or update the OLAP PostgreSQL role/user.
#   Create the OLAP database if it does not already exist.
#   Ensure the OLAP database owner is correct.
#
# Context:
#   The OLTP database/user is created automatically by the
#   official Postgres Docker image using POSTGRES_DB,
#   POSTGRES_USER and POSTGRES_PASSWORD.
#
#   The OLAP database/user is an extra project database/user,
#   so we create it manually during the bootstrap DAG.
# ============================================================

set -eu

# ------------------------------------------------------------
# 1. Validate required environment variables
# ------------------------------------------------------------

: "${SPORT_OLAP_DB:?SPORT_OLAP_DB is required}"
: "${SPORT_OLAP_USER:?SPORT_OLAP_USER is required}"
: "${SPORT_OLAP_PASSWORD:?SPORT_OLAP_PASSWORD is required}"

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

OLAP_DB="$SPORT_OLAP_DB"
OLAP_USER="$SPORT_OLAP_USER"
OLAP_PWD="$SPORT_OLAP_PASSWORD"

echo "Checking OLAP role and database..."
echo "OLAP database: ${OLAP_DB}"
echo "OLAP user: ${OLAP_USER}"

# ------------------------------------------------------------
# 2. Create or update OLAP role
# ------------------------------------------------------------

ROLE_EXISTS=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -U "$POSTGRES_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_roles WHERE rolname='${OLAP_USER}';")

if [ "$ROLE_EXISTS" = "1" ]; then
  echo "Role ${OLAP_USER} already exists. Updating password..."

  PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c \
    "ALTER ROLE ${OLAP_USER} WITH LOGIN PASSWORD '${OLAP_PWD}';"
else
  echo "Role ${OLAP_USER} does not exist. Creating role..."

  PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c \
    "CREATE ROLE ${OLAP_USER} LOGIN PASSWORD '${OLAP_PWD}';"
fi

# ------------------------------------------------------------
# 3. Create OLAP database if missing
# ------------------------------------------------------------

DB_EXISTS=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -U "$POSTGRES_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='${OLAP_DB}';")

if [ "$DB_EXISTS" != "1" ]; then
  echo "Database ${OLAP_DB} does not exist. Creating database..."

  PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c \
    "CREATE DATABASE ${OLAP_DB} OWNER ${OLAP_USER};"
else
  echo "Database ${OLAP_DB} already exists."
fi

# ------------------------------------------------------------
# 4. Ensure OLAP database owner is correct
# ------------------------------------------------------------

echo "Ensuring database ${OLAP_DB} is owned by ${OLAP_USER}..."

PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h localhost \
  -U "$POSTGRES_USER" \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -c "ALTER DATABASE ${OLAP_DB} OWNER TO ${OLAP_USER};"

echo "OLAP role and database bootstrap completed successfully."