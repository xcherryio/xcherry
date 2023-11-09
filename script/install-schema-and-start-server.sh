#!/bin/bash

CONFIG_PATH="${CONFIG_PATH:-/xdb/config/development-postgres.yaml}"
SRC_ROOT="${SRC_ROOT:-/xdb}"

"${SRC_ROOT}/xdb-tools-postgres" install-schema

"${SRC_ROOT}/xdb-tools-postgres" install-schema -f "${SRC_ROOT}/extensions/postgres/schema/sample_tables.sql"

"${SRC_ROOT}/xdb-server" --config "${CONFIG_PATH}" "$@"
