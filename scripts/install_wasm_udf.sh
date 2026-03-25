#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WASM_PATH="${WASM_PATH:-$ROOT_DIR/target/wasm32-unknown-unknown/release/hll_converter.wasm}"
FUNCTION_NAME="${FUNCTION_NAME:-apache_hll_to_uniqcombined64_state}"
MODULE_NAME="${MODULE_NAME:-hll_converter}"
EXPORT_NAME="${EXPORT_NAME:-apache_hll_to_uniqcombined64_state}"
BUILD_WASM="${BUILD_WASM:-1}"
REPLACE_MODULE="${REPLACE_MODULE:-1}"
CREATE_FUNCTION="${CREATE_FUNCTION:-1}"
ARGUMENTS_CLAUSE="${ARGUMENTS_CLAUSE:-(sketch String)}"
RETURNS_CLAUSE="${RETURNS_CLAUSE:-String}"
ABI_NAME="${ABI_NAME:-BUFFERED_V1}"

CH_CLIENT_BIN="${CH_CLIENT_BIN:-clickhouse-client}"
CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DATABASE="${CH_DATABASE:-default}"

if [[ "$FUNCTION_NAME" == *"'"* ]] || [[ "$MODULE_NAME" == *"'"* ]] || [[ "$EXPORT_NAME" == *"'"* ]]; then
  echo "single quotes are not supported in FUNCTION_NAME, MODULE_NAME, or EXPORT_NAME" >&2
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

if [[ "$CREATE_FUNCTION" == "1" ]]; then
  create_query="CREATE OR REPLACE FUNCTION ${FUNCTION_NAME} LANGUAGE WASM ARGUMENTS ${ARGUMENTS_CLAUSE} RETURNS ${RETURNS_CLAUSE} FROM '${MODULE_NAME}' :: '${EXPORT_NAME}' ABI ${ABI_NAME}"
  echo "Creating or replacing function '$FUNCTION_NAME'"
  if ! create_output="$(run_ch_query "$create_query" 2>&1)"; then
    server_version="$(run_ch_query "SELECT version()")"
    cat >&2 <<EOF
Failed to register the WebAssembly UDF with:
  $create_query

ClickHouse server version: $server_version
Error:
$create_output
EOF
    exit 1
  fi
fi

function_count="$(run_ch_query "SELECT count() FROM system.functions WHERE name = '$FUNCTION_NAME'")"
if [[ "$function_count" != "1" ]]; then
  echo "module upload succeeded, but function '$FUNCTION_NAME' is still not visible in system.functions" >&2
  exit 1
fi

echo "WebAssembly UDF is available as '$FUNCTION_NAME'"
