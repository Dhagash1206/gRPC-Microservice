#!/bin/sh
# Wraps the stock postgres entrypoint:
#   - copies the server key with the ownership and mode Postgres insists on
#     (mounted secrets are usually world-readable, which Postgres rejects)
#   - accepts only TLS 1.3 connections authenticated by a client certificate
#     whose CN equals the database role, so no passwords exist at all.
# Env: TLS_NAME (cert file basename under /certs)
set -eu

tls_dir=/var/lib/postgresql/tls
mkdir -p "$tls_dir"
cp "/certs/$TLS_NAME.crt" "$tls_dir/server.crt"
cp "/certs/$TLS_NAME.key" "$tls_dir/server.key"
cp /certs/ca.crt "$tls_dir/ca.crt"
cat > "$tls_dir/pg_hba.conf" <<'HBA'
# Unix socket inside the container only (init scripts, pg_isready).
local   all  all       trust
hostssl all  all  all  cert
HBA
chown -R postgres:postgres "$tls_dir"
chmod 700 "$tls_dir"
chmod 600 "$tls_dir/server.key"

exec docker-entrypoint.sh postgres \
  -c ssl=on \
  -c ssl_min_protocol_version=TLSv1.3 \
  -c "ssl_cert_file=$tls_dir/server.crt" \
  -c "ssl_key_file=$tls_dir/server.key" \
  -c "ssl_ca_file=$tls_dir/ca.crt" \
  -c "hba_file=$tls_dir/pg_hba.conf"
