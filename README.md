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

## Why stream processing

The obvious objection: why not just run a SQL query every few minutes?

Because three things here are impossible to reconstruct after the fact.

**A sale must be valued at the price that was live when it happened.** Not today's price —
*that* price. A batch job joining sales against a price table gets the current row and
silently misprices every sale that happened before the last markdown. Flink's temporal
join (`FOR SYSTEM_TIME AS OF`) looks up the version of `price_state` as of each sale's own
event time. This is the core of the project, and it is not expressible in a normal join.

**Order matters, and arrival order is not event order.** Two independent producers feed
this pipeline, so records arrive interleaved and late. Flink's watermarks let every
operator reason in *event* time, so a sale delayed by ten seconds still lands in the window
it belongs to. A batch job over a time column would quietly drop it or count it twice.

**"Sustained" is a temporal pattern, not a filter.** Overshoot is not "one bad window" —
it is a price drop followed by three consecutive high-velocity windows. That is a sequence
across rows, which `MATCH_RECOGNIZE` expresses directly and `WHERE` cannot.

And the value decays. A verdict 90 seconds after a markdown changes the next decision; the
same verdict tomorrow morning is a post-mortem.

---

## How Flink runs here

A **statement** is one SQL job. Submit it once and it runs forever, consuming from Kafka
and producing to Kafka, until you stop it. There is no schedule and no re-run — this is the
mental shift from batch.

Three consequences that shape the code:

- **Every table is a Kafka topic.** `CREATE TABLE` declares a topic plus its Avro schema;
  `INSERT INTO` starts a job that writes to it. They cannot be submitted together, which is
  why each file is a DDL/DML pair.
- **Statements are long-lived and stateful.** A `RUNNING` statement holds windows and
  pattern-matching state in memory. Redeploying one discards that state, so CEP output goes
  quiet for a warm-up period afterwards.
- **Jobs are chained through topics, not function calls.** Each statement reads the topic
  the previous one wrote. That is what makes the lineage graph a real graph.

### The statements

Seven jobs, each one file in [`flink/`](flink/), in dependency order:

| # | Statement | What it does | Why this operator |
|---|---|---|---|
| 01–02 | `price_state` | Latest price per product | **Upsert table.** A temporal join needs a *versioned* right side; an append-only topic cannot be one. |
| 03 | `sales_priced` | Values each sale at the price live at that moment | **Temporal join.** The whole thesis in one operator. |
| 04 | `demand_response` | Units and margin per product per minute | **1-minute tumbling window.** Turns individual sales into a demand *rate*. |
| 05 | `response_scored` | Flags statistically odd windows | **`ML_DETECT_ANOMALIES_ROBUST`** — multivariate, so it judges units and price together rather than one column at a time. |
| 06 | `overshoot_alerts` | Price drop → sustained high velocity | **`MATCH_RECOGNIZE`.** Confirmed, ~20–30 min. |
| 07 | `undershoot_alerts` | Price drop → demand that never moved | **`MATCH_RECOGNIZE`.** The failure mode where *nothing* looks wrong. |
| 09 | `response_verdicts` | intended lift vs observed lift | Provisional, ~1–2 min. The headline output. |

Each file's header comment explains the operator choice and the constraint that forced it.

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
| Environment | `env-53dw0n` |
| Kafka cluster | `lkc-9k5qo5y` |
| Schema Registry | `lsrc-j5zg2z2` (Essentials) |
| Flink compute pool | `lfcp-zmj9zj0` |

IDs change on every `recreate-all.sh` run — the live values are always in `infra.env`.

---

## Quickstart

Requires the `confluent` CLI, logged in.

```bash
# Corporate network only: set the proxy the CLI needs
source scripts/confluent-env.sh
confluent login --save

# Stand up everything: environment, cluster, topics, connectors, pool, Flink statements
# (8 SQL files -> 14 submitted statements: 7 CREATE TABLE + 7 long-running INSERT)
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
source infra.env
confluent flink shell \
  --compute-pool "$PG_POOL_ID" \
  --database "$PG_CLUSTER_ID" \
  --cloud aws --region us-east-1
```

`scripts/query.sh` exists for scripted checks: the Flink shell is interactive and cannot be
driven from a script, and `confluent flink statement create` only ever reports `RUNNING`
without showing rows.

### Verified output

From the live cluster
(`product_id, verdict, intended_lift, observed_lift, units_sold`):

```
["SKU-1004", "HEALTHY",    57, 48, 333]
["SKU-1002", "UNDERSHOOT", 39,  6, 240]
["SKU-1010", "HEALTHY",    40, 42, 375]
```

Row 2: the operator cut the price expecting a 39% lift and got 6%. The markdown was paid
for and the demand never arrived — margin given away for nothing.

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
docs/      architecture.md, outputs-explained.md, submission-answers.md,
           versioning-and-cicd.md
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
