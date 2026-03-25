#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WASM_PATH="${WASM_PATH:-$ROOT_DIR/target/wasm32-unknown-unknown/release/hll_converter.wasm}"
MODULE_NAME="${MODULE_NAME:-hll_converter}"
EXPORT_NAME="${EXPORT_NAME:-apache_hll_to_uniqcombined64_state}"
BUILD_WASM="${BUILD_WASM:-1}"
REPLACE_MODULE="${REPLACE_MODULE:-1}"

CH_CLIENT_BIN="${CH_CLIENT_BIN:-clickhouse-client}"
CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DATABASE="${CH_DATABASE:-default}"

if [[ "$MODULE_NAME" == *"'"* ]] || [[ "$EXPORT_NAME" == *"'"* ]]; then
  echo "single quotes are not supported in MODULE_NAME or EXPORT_NAME" >&2
  exit 1
fi

for tool in "$CH_CLIENT_BIN" xxd cargo; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    exit 1
  fi
done

CH_ARGS=(
  --host "$CH_HOST"
  --port "$CH_PORT"
  --user "$CH_USER"
  --database "$CH_DATABASE"
)

if [[ -n "$CH_PASSWORD" ]]; then
  CH_ARGS+=(--password "$CH_PASSWORD")
fi

run_ch_query() {
  local query="$1"
  "$CH_CLIENT_BIN" "${CH_ARGS[@]}" --query "$query"
}

module_exists() {
  local count
  count="$(run_ch_query "SELECT count() FROM system.webassembly_modules WHERE name = '$MODULE_NAME'")"
  [[ "$count" != "0" ]]
}

if [[ "$BUILD_WASM" == "1" ]]; then
  echo "Building wasm module"
  cargo build --release
fi

if [[ ! -f "$WASM_PATH" ]]; then
  echo "missing wasm artifact: $WASM_PATH" >&2
  exit 1
fi

if module_exists; then
  if [[ "$REPLACE_MODULE" != "1" ]]; then
    echo "module '$MODULE_NAME' already exists in system.webassembly_modules" >&2
    echo "Set REPLACE_MODULE=1 to delete and re-upload it." >&2
    exit 1
  fi

  echo "Deleting existing module '$MODULE_NAME'"
  run_ch_query "DELETE FROM system.webassembly_modules WHERE name = '$MODULE_NAME'"
fi

echo "Uploading $WASM_PATH into system.webassembly_modules as '$MODULE_NAME'"
{
  printf "INSERT INTO system.webassembly_modules (name, code) VALUES ('%s', unhex('" "$MODULE_NAME"
  xxd -p -c 0 "$WASM_PATH" | tr -d '\n'
  printf "'))\n"
} | "$CH_CLIENT_BIN" "${CH_ARGS[@]}"

module_hash="$(run_ch_query "SELECT hash FROM system.webassembly_modules WHERE name = '$MODULE_NAME'")"
echo "Uploaded module hash: $module_hash"

function_count="$(run_ch_query "SELECT count() FROM system.functions WHERE name = '$EXPORT_NAME'")"
if [[ "$function_count" == "1" ]]; then
  echo "WebAssembly UDF is available as '$EXPORT_NAME'"
  exit 0
fi

server_version="$(run_ch_query "SELECT version()")"
cat >&2 <<EOF
Module upload succeeded, but '$EXPORT_NAME' is not visible in system.functions yet.

On this server ($server_version), the module storage path is confirmed:
  INSERT INTO system.webassembly_modules (name, code) VALUES ('${MODULE_NAME}', unhex('<hex wasm>'))

However, this build does not auto-register the exported function after upload, and the
local parser rejects CREATE FUNCTION statements entirely. If your production ClickHouse
build supports WebAssembly UDF registration separately, use the uploaded module
'$MODULE_NAME' as the source module and bind the exported symbol '$EXPORT_NAME' there.
EOF
exit 1
