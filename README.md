# PriceGuard

A markdown control loop on Confluent Cloud.

PriceGuard answers one question, roughly ninety seconds after a price change lands: **did
that change produce the demand response the operator intended?** It emits OVERSHOOT,
UNDERSHOOT or HEALTHY per product per minute, quantifying the gap between intended and
observed demand lift.

---

## Thesis

> A price change isn't a fact. It's a stimulus in a feedback loop. You change a price
> expecting a demand response. The response arrives over the next minutes. Your next
> decision depends on whether it matched intent. That's a control loop — and control loops
> require feedback arriving faster than the system drifts.

Retail closes that loop in a nightly batch report, which is to say it does not close it.

---

## Architecture

```
   datagen-price-changes              datagen-sales-events
             |                                  |
             v                                  v
     +----------------+                +----------------+
     | price-changes  |                | sales-events   |   Avro, Schema Registry
     +----------------+                +----------------+
             | [1][2]                           |
             v                                  |
     +----------------+                         |
     |  price_state   |  UPSERT, PK product_id  |
     +----------------+                         |
             |                                  |
             +-------------+     +--------------+
                           v     v
                  +-------------------------+
                  |      sales_priced       |  [3] TEMPORAL JOIN
                  |  FOR SYSTEM_TIME AS OF  |
                  +-------------------------+
                                |
                                v
                  +-------------------------+
                  |    demand_response      |  [4] TUMBLE 1 MINUTE
                  +-------------------------+
                        |                |
                        |                v  [5] ML_DETECT_ANOMALIES_ROBUST
                        |      +-------------------------+
                        |      |     response_scored     |
                        |      +-------------------------+
                        |          |           |
                        | [9]      | [6]       | [7]   MATCH_RECOGNIZE
                        v          v           v
            +-------------------+  +----------------+  +-----------------+
            | response_verdicts |  | overshoot_     |  | undershoot_     |
            |   PROVISIONAL     |  |   alerts       |  |   alerts        |
            |   ~1-2 min        |  |   CONFIRMED    |  |   CONFIRMED     |
            +-------------------+  +----------------+  +-----------------+
                   TIER 1                        TIER 2
```

Full explanation in [`docs/architecture.md`](docs/architecture.md).

**The two tiers.** `response_verdicts` gives a PROVISIONAL verdict from ONE window
(~1–2 min). The `MATCH_RECOGNIZE` statements give a CONFIRMED verdict from a SUSTAINED
sequence (~20–30 min). A control loop that waits for certainty has already missed the
decision window.

---

## Deployed infrastructure

| Resource | Identifier |
|---|---|
| Cloud / region | AWS `us-east-1` |
| Organization | Walmart |
| Environment | `env-poxrrk` |
| Kafka cluster | `lkc-pgw3j8k` |
| Schema Registry | `lsrc-v78z63n` (Essentials) |
| Flink compute pool | `lfcp-125m5jj` |

---

## Quickstart

Requires the `confluent` CLI, logged in.

```bash
# Corporate network only: set the proxy the CLI needs
source scripts/confluent-env.sh
confluent login --save

# Stand up everything: environment, cluster, topics, connectors, pool, 9 Flink statements
./scripts/recreate-all.sh

# Run an ad-hoc query against the running pipeline
./scripts/query.sh "SELECT * FROM response_verdicts LIMIT 10;"

# Delete everything. Idle compute pools and clusters bill continuously.
./scripts/teardown.sh
```

`recreate-all.sh` writes `infra.env` with the generated resource IDs. It is gitignored and
must never be committed.

---

## Viewing output

| Method | Good for |
|---|---|
| `confluent flink shell` | Canonical CLI path. Live-updating result table in the terminal. |
| Cloud UI → Flink → SQL Workspace | Best visual. Live result grid, shareable. |
| Cloud UI → Topics → Messages | Raw records. No SQL needed. |
| Data Portal | Topics as governed data products. |

```bash
confluent flink shell \
  --compute-pool lfcp-125m5jj \
  --database lkc-pgw3j8k \
  --cloud aws --region us-east-1
```

`scripts/query.sh` exists for scripted checks: the Flink shell is interactive and cannot be
driven from a script, and `confluent flink statement create` only ever reports `RUNNING`
without showing rows.

### Verified output

From the live cluster
(`product_id, verdict, intended_lift, observed_lift, units_sold, total_margin`):

```
["SKU-1001", "OVERSHOOT",  41, 97, 340, 5454908]
["SKU-1004", "HEALTHY",    47, 74, 264, 2927215]
["SKU-1009", "UNDERSHOOT", 57,  0,  97,  302027]
```

Row 1: intended a 41% lift, got 97%. Overshoot — margin left on the table.

---

## Constraints discovered by trial

| Constraint | Detail |
|---|---|
| Reluctant quantifier required | `MATCH_RECOGNIZE` rejects a greedy quantifier as the last pattern element. Use `{2,}?`, not `{2,}`. Not in the docs. |
| DDL and DML must be separate | `CREATE TABLE` and `INSERT INTO` cannot be submitted as one Flink statement. |
| Event time only | Confluent Cloud Flink does not support processing time. |
| Temporal join needs a versioned right side | Hence the `price_state` UPSERT table; an append-only topic cannot be the right side of `FOR SYSTEM_TIME AS OF`. |
| Upsert bucket key = primary key | `DISTRIBUTED BY (product_id)` on `price_state` is mandatory. |
| HTTP Sink V2 topic routing | Needs `api1.topics` in addition to connector-level `topics`; `behavior.on.error` must be lowercase. (Sink abandoned — webhook.site free tier caps at 50 requests.) |
| Local Kafka consume fails behind the corporate proxy | Broker port 9092 cannot traverse an HTTP proxy. Flink runs server-side and is unaffected, so all verification goes through Flink SQL. |
| CLI proxy | `proxy-intlho.wal-mart.com:8080` works; `sysproxy.wal-mart.com:8080` returns HTTP 407. |

---

## Repository layout

```
flink/     Flink SQL statements, numbered in dependency order
schemas/   Custom Avro schemas driving the Datagen connectors
scripts/   recreate-all.sh, query.sh, teardown.sh, confluent-env.sh
docs/      architecture.md, outputs-explained.md, submission-answers.md
```

---

## Governance

Stream Governance stays on **Essentials**, deliberately. Essentials already provides Schema
Registry with Avro, auto-registered subjects, compatibility enforcement, Stream Lineage
(10-minute window), Catalog tags and the Data Portal. The headline Advanced feature is Data
Contract rules — and per Confluent's documentation, schema rules do not execute in Kafka
Connect or Flink SQL. Since this application's write path is entirely Connect and its
processing path is entirely Flink SQL, an Advanced upgrade would buy a feature that is
structurally inert here.
