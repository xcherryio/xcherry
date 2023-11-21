#!/bin/bash

# the script is used to start xcherry-server in docker-compose

CONFIG_PATH="${CONFIG_PATH:-/xcherry/config/development-postgres.yaml}"
SRC_ROOT="${SRC_ROOT:-/xcherry}"

"${SRC_ROOT}/xcherry-tools-postgres" --endpoint "postgres" install-schema

"${SRC_ROOT}/xcherry-tools-postgres" --endpoint "postgres" install-schema -f "${SRC_ROOT}/extensions/postgres/schema/sample_tables.sql"

"${SRC_ROOT}/xcherry-server" --config "${CONFIG_PATH}" "$@"
