#!/bin/sh
# Runs once on first start (docker-entrypoint-initdb.d). Creates the service's
# login role (no password; authenticated by client certificate) owning its database.
# Env: APP_ROLE, APP_DATABASE
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE "$APP_ROLE" LOGIN;
CREATE DATABASE "$APP_DATABASE" OWNER "$APP_ROLE";
SQL
