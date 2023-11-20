#!/bin/bash

CONFIG_PATH="${CONFIG_PATH:-/xcherry/config/development-postgres.yaml}"
SRC_ROOT="${SRC_ROOT:-/xcherry}"

"${SRC_ROOT}/xcherry-server" --config "${CONFIG_PATH}" "$@"
