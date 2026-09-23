#!/usr/bin/env bash
#
# recreate-all.sh - stand up the entire PriceGuard pipeline from nothing.
#
# Idempotent-ish: it creates fresh resources every run. If you already have an
# environment named "priceguard", tear it down first with scripts/teardown.sh.
#
# Usage:
#   ./scripts/recreate-all.sh
#
# Prereqs: confluent CLI logged in (confluent login --save)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

# Walmart corporate network: confluent.cloud does not resolve without this proxy.
# shellcheck source=/dev/null
source "$SCRIPT_DIR/confluent-env.sh" >/dev/null

ENV_NAME="priceguard"
CLUSTER_NAME="priceguard-cluster"
POOL_NAME="priceguard-pool"
CLOUD="aws"
REGION="us-east-1"

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
ok()   { printf '    \033[0;32mOK\033[0m %s\n' "$*"; }
die()  { printf '\n\033[0;31mFAILED: %s\033[0m\n' "$*" >&2; exit 1; }

# jget <field> [filter_field=value]
#
# Every `confluent ... --output json` call here needs exactly one field pulled
# back out. This replaces the half-dozen copies of the same python one-liner
# that used to be smeared across this script. Reads a JSON object (or an array
# of them) on stdin and prints <field>, one line per matching element. Prints
# nothing when the field is absent, so callers test for emptiness rather than
# relying on a stack trace.
jget() {
  python3 -c '
import sys, json
raw = sys.stdin.read()
try:
    doc = json.loads(raw)
except Exception:
    # Some confluent subcommands append a human-readable line AFTER the JSON
    # even under --output json. `api-key create --use` is the one that bit us:
    # it prints the {api_key, api_secret} object and then
    #     Using API Key "XXXX".
    # json.loads sees that trailing line as "Extra data" and raises, jget exits
    # non-zero, and under `set -euo pipefail` the whole script dies with no
    # message - after the key has already been created in the cloud.
    # raw_decode parses the leading JSON value and simply ignores whatever
    # follows it, which is exactly the tolerance we want.
    try:
        doc, _ = json.JSONDecoder().raw_decode(raw.lstrip())
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

# wait_for <want> <tries> <delay> <describe command...>
#
# The cluster and the compute pool are provisioned asynchronously and poll
# identically. They used to be two copy-pasted loops, and the copy forgot its
# post-loop assertion - so a FAILED pool still printed "pool ready" and the run
# carried on burning money. One helper, one assertion, no way to forget it.
wait_for() {
  local want="$1" tries="$2" delay="$3"
  shift 3
  local status=""
  printf '    waiting for %s' "$want"
  for _ in $(seq 1 "$tries"); do
    status=$("$@" 2>/dev/null | jget status 2>/dev/null || echo "")
    [[ "$status" == "$want" ]] && break
    printf '.'; sleep "$delay"
  done
  printf '\n'
  [[ "$status" == "$want" ]] || die "expected status $want, last seen was '${status:-unknown}'"
}

# ---------------------------------------------------------------------------
log "1/9  Environment"
ENV_ID=$(confluent environment create "$ENV_NAME" --output json 2>/dev/null | jget id) \
  || die "could not create environment (does '$ENV_NAME' already exist? run teardown.sh)"
[[ -n "$ENV_ID" ]] || die "environment create returned no id (does '$ENV_NAME' already exist? run teardown.sh)"
confluent environment use "$ENV_ID" >/dev/null
ok "$ENV_ID"

# ---------------------------------------------------------------------------
log "2/9  Kafka cluster (also auto-enables Schema Registry)"
CLUSTER_ID=$(confluent kafka cluster create "$CLUSTER_NAME" \
  --cloud "$CLOUD" --region "$REGION" --type basic --output json | jget id)
[[ -n "$CLUSTER_ID" ]] || die "cluster create returned no id"
ok "$CLUSTER_ID  (provisioning)"

wait_for UP 40 15 confluent kafka cluster describe "$CLUSTER_ID" --output json
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
ok "cluster UP"

# ---------------------------------------------------------------------------
log "3/9  Kafka API key"
KEY_JSON=$(confluent api-key create --resource "$CLUSTER_ID" \
  --description "priceguard-kafka" --use --output json)
API_KEY=$(printf '%s' "$KEY_JSON"    | jget api_key)
API_SECRET=$(printf '%s' "$KEY_JSON" | jget api_secret)
[[ -n "$API_KEY" && -n "$API_SECRET" ]] || die "Kafka api-key create returned no key/secret"
ok "$API_KEY"

# API keys take a few seconds to propagate before connectors can use them.
sleep 20

# ---------------------------------------------------------------------------
log "4/9  Flink API key"
#
# This is a SEPARATE credential from the cluster key above: Flink keys are
# scoped to a cloud+region, not to a cluster, and the SQL REST API that
# scripts/query.sh drives will not accept the Kafka key. Both end up in
# infra.env; do not try to use one for the other.
FLINK_KEY_JSON=$(confluent api-key create --resource flink \
  --cloud "$CLOUD" --region "$REGION" \
  --description "priceguard-flink-rest" --output json) \
  || die "could not create a Flink API key for $CLOUD/$REGION"
FLINK_API_KEY=$(printf '%s' "$FLINK_KEY_JSON"    | jget api_key)
FLINK_API_SECRET=$(printf '%s' "$FLINK_KEY_JSON" | jget api_secret)
[[ -n "$FLINK_API_KEY" && -n "$FLINK_API_SECRET" ]] || die "Flink api-key create returned no key/secret"
ok "$FLINK_API_KEY"

# ---------------------------------------------------------------------------
log "5/9  Topics"
#
# "already exists" is benign on a re-run; an auth or quota failure is not. The
# old `>/dev/null 2>&1 && ok || ok "(exists)"` cheerfully reported OK for both.
for t in price-changes sales-events; do
  if TOPIC_OUT=$(confluent kafka topic create "$t" --partitions 3 2>&1); then
    ok "$t"
  elif printf '%s' "$TOPIC_OUT" | grep -qi 'already exists'; then
    ok "$t (exists)"
  else
    die "could not create topic $t: $(printf '%s' "$TOPIC_OUT" | tr '\n' ' ')"
  fi
done

# ---------------------------------------------------------------------------
log "6/9  Connector configs (secrets injected at runtime, never committed)"
#
# connectors/ is gitignored down to the last .json and git cannot track an empty
# directory, so on a fresh clone it simply does not exist. Creating it here
# beats a FileNotFoundError thrown *after* we have already provisioned billable
# resources.
mkdir -p connectors

# Secrets go in through the environment, never on the command line - argv is
# world-readable via `ps auxww`. The heredoc is quoted so the shell cannot
# interpolate them either.
API_KEY="$API_KEY" API_SECRET="$API_SECRET" \
python3 - <<'PY'
import json, os
key, secret = os.environ['API_KEY'], os.environ['API_SECRET']

def datagen(schema_file, topic, name, interval):
    return {
        "connector.class": "DatagenSource", "name": name,
        "kafka.auth.mode": "KAFKA_API_KEY",
        "kafka.api.key": key, "kafka.api.secret": secret,
        "kafka.topic": topic, "output.data.format": "AVRO",
        "schema.string": json.dumps(json.load(open(schema_file)), separators=(',', ':')),
        "schema.keyfield": "product_id",
        "max.interval": interval, "tasks.max": "1",
    }

cfgs = {
    "connectors/datagen-price-changes.json":
        datagen("schemas/price-changes.datagen.avsc", "price-changes", "datagen-price-changes", "3000"),
    "connectors/datagen-sales-events.json":
        datagen("schemas/sales-events.datagen.avsc", "sales-events", "datagen-sales-events", "300"),
}
for path, cfg in cfgs.items():
    json.dump(cfg, open(path, 'w'), indent=2)
    print(f"    wrote {path}")
PY

# ---------------------------------------------------------------------------
log "7/9  Source connectors"
for c in datagen-price-changes datagen-sales-events; do
  confluent connect cluster create --config-file "connectors/$c.json" >/dev/null
  ok "$c"
done

# ---------------------------------------------------------------------------
log "8/9  Flink compute pool"
POOL_ID=$(confluent flink compute-pool create "$POOL_NAME" \
  --cloud "$CLOUD" --region "$REGION" --max-cfu 10 --output json | jget id)
[[ -n "$POOL_ID" ]] || die "compute-pool create returned no id"
ok "$POOL_ID  (provisioning)"

wait_for PROVISIONED 30 10 confluent flink compute-pool describe "$POOL_ID" --output json
ok "pool ready"

# ---------------------------------------------------------------------------
log "9/9  Flink statements"
#
# Two rules learned the hard way:
#  - Confluent Flink rejects a combined DDL+DML submission, so every .sql file
#    that contains an INSERT is split at the INSERT keyword and submitted twice.
#  - Statements are strictly order-dependent; --wait enforces the sequencing.
#
submit() {
  local name="$1" sql="$2"
  confluent flink statement create "$name" --sql "$sql" \
    --compute-pool "$POOL_ID" --database "$CLUSTER_ID" \
    --cloud "$CLOUD" --region "$REGION" --wait >/dev/null 2>&1 \
    && ok "$name" || die "statement $name failed - inspect with: confluent flink statement describe $name --cloud $CLOUD --region $REGION"
}

# has_sql - true only if the argument contains something other than `--` comment
# lines and blank lines. Files like 02-price-state-insert.sql lead with a
# comment header, so the pre-INSERT half is non-empty but is not a statement;
# submitting it gets rejected by Confluent and aborts the whole run.
has_sql() {
  [[ -n "$(printf '%s\n' "$1" | sed -e '/^[[:space:]]*--/d' -e '/^[[:space:]]*$/d')" ]]
}

for f in flink/[0-9]*.sql; do
  base=$(basename "$f" .sql)
  if grep -q 'INSERT INTO' "$f"; then
    ddl=$(python3 -c "import sys;t=open(sys.argv[1]).read();i=t.index('INSERT INTO');print(t[:i].strip())" "$f")
    dml=$(python3 -c "import sys;t=open(sys.argv[1]).read();i=t.index('INSERT INTO');print(t[i:].strip())" "$f")
    if has_sql "$ddl"; then
      submit "pg-${base}-ddl" "$ddl"
    fi
    submit "pg-${base}-dml" "$dml"
  else
    submit "pg-${base}" "$(cat "$f")"
  fi
done

# ---------------------------------------------------------------------------
# infra.env carries live API secrets, so it must not inherit an ambient umask of
# 022 and land world-readable. Recreate it from scratch: `>` on an existing file
# keeps the old (possibly loose) permissions.
rm -f infra.env
(
  umask 077
  cat > infra.env <<EOF
# PriceGuard infra - generated $(date '+%Y-%m-%d %H:%M:%S') - NEVER COMMIT
export PG_ENV_ID=$ENV_ID
export PG_CLUSTER_ID=$CLUSTER_ID
export PG_POOL_ID=$POOL_ID
export PG_CLOUD=$CLOUD
export PG_REGION=$REGION
export PG_KAFKA_API_KEY='$API_KEY'
export PG_KAFKA_API_SECRET='$API_SECRET'
# Region-scoped Flink key - this is the one scripts/query.sh authenticates with.
export PG_FLINK_API_KEY='$FLINK_API_KEY'
export PG_FLINK_API_SECRET='$FLINK_API_SECRET'
EOF
)

printf '\n\033[1;32m=== PriceGuard is live ===\033[0m\n\n'
printf '  Environment : %s\n' "$ENV_ID"
printf '  Cluster     : %s\n' "$CLUSTER_ID"
printf '  Flink pool  : %s\n' "$POOL_ID"
printf '  Console     : https://confluent.cloud/environments/%s/clusters/%s\n' "$ENV_ID" "$CLUSTER_ID"
printf '  IDs saved   : infra.env (gitignored, mode 600)\n\n'
printf '  Wait ~5 min for ARIMA warm-up, then screenshot Stream Lineage.\n'
printf '  Lineage renders only the LAST 10 MINUTES of traffic.\n\n'
