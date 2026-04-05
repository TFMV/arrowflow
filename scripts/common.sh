#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLOW_ROOT="$(dirname "$SCRIPT_DIR")"
BIN="$ARROWFLOW_ROOT/bin/arrowflow"
TELEMETRY_FILE="$ARROWFLOW_ROOT/telemetry.json"

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
    return
  fi
  echo "docker compose is required" >&2
  return 1
}

ensure_binary() {
  mkdir -p "$(dirname "$BIN")"
  (
    cd "$ARROWFLOW_ROOT"
    go build -o "$BIN" ./cmd/arrowflow
  )
}

summary_value() {
  local file="$1"
  local label="$2"
  awk -F': ' -v label="$label" '
    $1 ~ "^[[:space:]]*" label "$" {
      sub(/^[[:space:]]+/, "", $2)
      print $2
      exit
    }
  ' "$file"
}

line_for_label() {
  local file="$1"
  local label="$2"
  awk -v label="$label" '
    $0 ~ "^[[:space:]]*" label ":" {
      print
      exit
    }
  ' "$file"
}

latency_value() {
  local file="$1"
  local label="$2"
  local key="$3"
  local line
  line="$(line_for_label "$file" "$label")"
  if [[ -z "$line" ]]; then
    echo "0"
    return
  fi
  sed -E "s/.*${key}=([0-9.]+).*/\\1/" <<<"$line"
}

peak_value() {
  local file="$1"
  local label="$2"
  local line
  line="$(line_for_label "$file" "$label")"
  if [[ -z "$line" ]]; then
    echo "0"
    return
  fi
  sed -E 's/.*Peak: ([0-9]+).*/\1/' <<<"$line"
}

scalar_value() {
  local file="$1"
  local label="$2"
  local line
  line="$(line_for_label "$file" "$label")"
  if [[ -z "$line" ]]; then
    echo "0"
    return
  fi
  sed -E 's/.*:[[:space:]]*([0-9.]+).*/\1/' <<<"$line"
}

avg_batch_rows() {
  local file="$1"
  local line
  line="$(line_for_label "$file" "Avg Batch")"
  if [[ -z "$line" ]]; then
    echo "0"
    return
  fi
  sed -E 's/.*Avg Batch:[[:space:]]*([0-9.]+) rows.*/\1/' <<<"$line"
}

avg_batch_kb() {
  local file="$1"
  local line
  line="$(line_for_label "$file" "Avg Batch")"
  if [[ -z "$line" ]]; then
    echo "0"
    return
  fi
  sed -E 's/.*\(([0-9.]+) KB\).*/\1/' <<<"$line"
}

telemetry_value() {
  local file="$1"
  local path="$2"
  python3 - "$file" "$path" <<'PY'
import json
import sys

file_path, path = sys.argv[1], sys.argv[2]
try:
    with open(file_path, "r", encoding="utf-8") as fh:
        value = json.load(fh)
except FileNotFoundError:
    print("0")
    raise SystemExit(0)

for part in path.split("."):
    if not isinstance(value, dict):
        value = 0
        break
    value = value.get(part, 0)

if isinstance(value, float):
    print(f"{value:.6f}")
else:
    print(value)
PY
}

duration_to_seconds() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import re
import sys

raw = sys.argv[1].strip()
if not raw:
    print("0")
    raise SystemExit(0)

total = 0.0
for value, unit in re.findall(r'([0-9]+(?:\.[0-9]+)?)(ms|us|ns|h|m|s)', raw):
    value = float(value)
    if unit == "h":
        total += value * 3600
    elif unit == "m":
        total += value * 60
    elif unit == "s":
        total += value
    elif unit == "ms":
        total += value / 1000
    elif unit == "us":
        total += value / 1_000_000
    elif unit == "ns":
        total += value / 1_000_000_000
print(f"{total:.6f}")
PY
}

run_arrowflow() {
  local output_file="$1"
  shift

  rm -f "$TELEMETRY_FILE"
  (
    cd "$ARROWFLOW_ROOT"
    "$BIN" "$@"
  ) >"$output_file" 2>&1
}

wait_for_http() {
  local port="$1"
  local retries="${2:-60}"
  for ((i = 1; i <= retries; i++)); do
    if curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "NATS monitoring endpoint on port ${port} did not become ready" >&2
  return 1
}
