#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/testdata/apache_hll}"
MANIFEST_PATH="$OUT_DIR/manifest.tsv"
FUNCTION_NAME="${FUNCTION_NAME:-apache_hll_to_uniqcombined64_state}"
LG_K="${LG_K:-12}"
CH_CLIENT_BIN="${CH_CLIENT_BIN:-clickhouse-client}"
CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DATABASE="${CH_DATABASE:-default}"

if ! command -v "$CH_CLIENT_BIN" >/dev/null 2>&1; then
  echo "missing clickhouse-client binary: $CH_CLIENT_BIN" >&2
  exit 1
fi

if ! command -v xxd >/dev/null 2>&1; then
  echo "missing xxd" >&2
  exit 1
fi

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

echo "Generating Apache HLL fixtures into $OUT_DIR"
cargo run --target x86_64-unknown-linux-gnu --bin generate_apache_hll_fixtures -- --out-dir "$OUT_DIR"

if [[ ! -f "$MANIFEST_PATH" ]]; then
  echo "fixture manifest was not generated: $MANIFEST_PATH" >&2
  exit 1
fi

function_count="$(run_ch_query "SELECT count() FROM system.functions WHERE name = '$FUNCTION_NAME'")"
if [[ "$function_count" != "1" ]]; then
  echo "ClickHouse function '$FUNCTION_NAME' is not registered on $CH_HOST:$CH_PORT" >&2
  echo "Upload the WASM UDF first, then rerun this script." >&2
  exit 1
fi

echo "Verifying converted states with finalizeAggregation()"

passed=0
failed=0

while IFS=$'\t' read -r file_name mode inserted_count tolerance_abs; do
  fixture_path="$OUT_DIR/$file_name"
  if [[ ! -f "$fixture_path" ]]; then
    echo "missing fixture: $fixture_path" >&2
    failed=$((failed + 1))
    continue
  fi

  fixture_hex="$(xxd -p -c 0 "$fixture_path" | tr -d '\n')"
  query="
WITH
    CAST($FUNCTION_NAME(unhex('$fixture_hex')), 'AggregateFunction(uniqCombined64($LG_K), UInt64)') AS converted_state
SELECT toFloat64(finalizeAggregation(converted_state))
"
  actual_value="$(run_ch_query "$query")"
  rounded_value="$(printf '%.0f' "$actual_value")"

  abs_error=$(( rounded_value >= inserted_count ? rounded_value - inserted_count : inserted_count - rounded_value ))
  if (( abs_error <= tolerance_abs )); then
    printf 'PASS  %-24s mode=%-5s expected=%s actual=%s abs_error=%s tolerance=%s\n' \
      "$file_name" "$mode" "$inserted_count" "$rounded_value" "$abs_error" "$tolerance_abs"
    passed=$((passed + 1))
  else
    printf 'FAIL  %-24s mode=%-5s expected=%s actual=%s abs_error=%s tolerance=%s\n' \
      "$file_name" "$mode" "$inserted_count" "$rounded_value" "$abs_error" "$tolerance_abs" >&2
    failed=$((failed + 1))
  fi
done < <(tail -n +2 "$MANIFEST_PATH")

if (( failed > 0 )); then
  echo "verification failed: passed=$passed failed=$failed" >&2
  exit 1
fi

echo "verification passed: $passed fixtures"
