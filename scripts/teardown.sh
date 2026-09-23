#!/usr/bin/env bash
#
# teardown.sh - delete everything PriceGuard created.
#
# RUN THIS WHEN DONE. Idle Flink compute pools and Kafka clusters bill continuously.
#
# Deleting the environment cascades to the cluster, connectors and pool, but we
# delete children explicitly first so that a child which refuses to go away is
# reported rather than swallowed - every delete below prints the CLI's own error
# text on failure and keeps going, because a half-torn-down environment still
# bills and we would rather finish the sweep than stop at the first casualty.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

# shellcheck source=/dev/null
source "$SCRIPT_DIR/confluent-env.sh" >/dev/null

# jget <field> [filter_field=value]
#
# Same contract as the helper in recreate-all.sh: read JSON (object or array of
# objects) on stdin, print one <field> per matching element, print nothing if
# the field is missing. Beats scattering the same python one-liner over every
# `--output json` call site.
jget() {
  python3 -c '
import sys, json
try:
    doc = json.load(sys.stdin)
except Exception:
    sys.exit(1)
field = sys.argv[1]
fkey = fval = None
if len(sys.argv) > 2:
    fkey, _, fval = sys.argv[2].partition("=")
for item in (doc if isinstance(doc, list) else [doc]):
    if not isinstance(item, dict):
        continue
    if fkey is not None and str(item.get(fkey, "")) != fval:
        continue
    if field in item:
        print(item[field])
' "$@"
}

# try_delete <label> <command...>
#
# Deletion is best-effort but never silent: on failure we echo what the CLI
# actually said, so "teardown complete" cannot mean "teardown quietly failed".
try_delete() {
  local label="$1"
  shift
  local out
  if out=$("$@" 2>&1); then
    echo "    deleted $label"
  else
    echo "    !! could not delete $label: $(printf '%s' "$out" | tr '\n' ' ')" >&2
  fi
}

if [[ -f infra.env ]]; then
  # shellcheck source=/dev/null
  source infra.env
else
  echo "infra.env not found - resolving environment by name instead."
  PG_ENV_ID=$(confluent environment list --output json 2>/dev/null | jget id name=priceguard)
  [[ -z "${PG_ENV_ID:-}" ]] && { echo "No 'priceguard' environment found. Nothing to do."; exit 0; }
fi

echo "About to DELETE environment $PG_ENV_ID and everything inside it."
read -r -p "Type 'yes' to confirm: " confirm
[[ "$confirm" == "yes" ]] || { echo "Aborted."; exit 0; }

confluent environment use "$PG_ENV_ID" >/dev/null 2>&1

echo "==> Flink statements"
confluent flink statement list --cloud "${PG_CLOUD:-aws}" --region "${PG_REGION:-us-east-1}" --output json 2>/dev/null \
  | jget name 2>/dev/null \
  | while read -r s; do
      [[ -n "$s" ]] && try_delete "$s" confluent flink statement delete "$s" \
        --cloud "${PG_CLOUD:-aws}" --region "${PG_REGION:-us-east-1}" --force
    done

echo "==> Flink compute pool"
[[ -n "${PG_POOL_ID:-}" ]] && try_delete "$PG_POOL_ID" confluent flink compute-pool delete "$PG_POOL_ID" --force

echo "==> Connectors"
confluent connect cluster list --output json 2>/dev/null \
  | jget id 2>/dev/null \
  | while read -r c; do
      [[ -n "$c" ]] && try_delete "$c" confluent connect cluster delete "$c" --force
    done

echo "==> Kafka cluster"
[[ -n "${PG_CLUSTER_ID:-}" ]] && try_delete "$PG_CLUSTER_ID" confluent kafka cluster delete "$PG_CLUSTER_ID" --force

echo "==> Environment"
try_delete "$PG_ENV_ID" confluent environment delete "$PG_ENV_ID" --force

rm -f infra.env connectors/*.json
echo
echo "Teardown complete. Local generated artifacts removed."
echo "Recreate any time with: ./scripts/recreate-all.sh"
