#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/testdata/apache_hll}"
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
cargo run --target x86_64-unknown-linux-gnu --bin generate_apache_hll_fixtures -- --out-dir "$OUT_DIR" >/dev/null

function_count="$(run_ch_query "SELECT count() FROM system.functions WHERE name = '$FUNCTION_NAME'")"
if [[ "$function_count" != "1" ]]; then
  echo "ClickHouse function '$FUNCTION_NAME' is not registered on $CH_HOST:$CH_PORT" >&2
  exit 1
fi

printf '%-28s %12s %16s %16s %12s %12s %12s\n' \
  "fixture" "inserted" "apache_est" "clickhouse" "apache_err%" "ch_err%" "ch-vs-apache"

while IFS=$'\t' read -r file_name mode inserted_count tolerance_abs apache_estimate; do
  fixture_path="$OUT_DIR/$file_name"
  fixture_hex="$(xxd -p -c 0 "$fixture_path" | tr -d '\n')"
  query="
WITH
    CAST($FUNCTION_NAME(unhex('$fixture_hex')), 'AggregateFunction(uniqCombined64($LG_K), UInt64)') AS converted_state
SELECT toFloat64(finalizeAggregation(converted_state))
"
  clickhouse_estimate="$(run_ch_query "$query")"
  python3 - "$file_name" "$inserted_count" "$apache_estimate" "$clickhouse_estimate" <<'PY'
import sys

file_name, inserted, apache_est, clickhouse_est = sys.argv[1:]
inserted = float(inserted)
apache_est = float(apache_est)
clickhouse_est = float(clickhouse_est)

def pct(est):
    if inserted == 0:
        return 0.0
    return ((est - inserted) / inserted) * 100.0

print(f"{file_name:<28} {inserted:12.0f} {apache_est:16.6f} {clickhouse_est:16.6f} {pct(apache_est):12.4f} {pct(clickhouse_est):12.4f} {clickhouse_est - apache_est:12.6f}")
PY
done < <(cargo run --target x86_64-unknown-linux-gnu --bin print_apache_hll_estimates -- --out-dir "$OUT_DIR")
