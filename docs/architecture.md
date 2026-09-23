# PriceGuard — Architecture

A markdown control loop on Confluent Cloud.

---

## 1. Thesis

> A price change isn't a fact. It's a stimulus in a feedback loop. You change a price
> expecting a demand response. The response arrives over the next minutes. Your next
> decision depends on whether it matched intent. That's a control loop — and control loops
> require feedback arriving faster than the system drifts.

Every design decision below follows from that sentence. The system does not ask "is this
price valid?" It asks "did this price change produce the demand response we intended?"

To answer that, three things must be true at once:

1. Every sale must be valued at the price that was live **at the moment of the sale**.
2. Evidence must be evaluated as an **ordered sequence**, not as an unordered aggregate.
3. The verdict must arrive **faster than the operator's next decision**.

Item 1 is a temporal join. Item 2 is pattern matching over an event-time stream. Item 3
rules out batch. Section 6 expands on this.

---

## 2. Deployed infrastructure

| Resource | Identifier |
|---|---|
| Cloud / region | AWS `us-east-1` |
| Organization | Walmart |
| Environment | `env-poxrrk` |
| Kafka cluster | `lkc-pgw3j8k` |
| Schema Registry | `lsrc-v78z63n` (Essentials package) |
| Flink compute pool | `lfcp-125m5jj` |

### Connectors

| Connector | Type | Topic | Purpose |
|---|---|---|---|
| `datagen-price-changes` | Datagen Source | `price-changes` | Price moves, each carrying `intended_lift_pct` — the operator's stated bet |
| `datagen-sales-events` | Datagen Source | `sales-events` | Individual sales, produced independently of the price feed and correlated only by event time |

Both use a **custom Avro schema** supplied through `schema.string`, not a built-in
quickstart template. The custom schema is what makes the data answerable: it plants a
declared `intended_lift_pct` on every price change, so the pipeline can compare intent
against outcome rather than guessing a threshold.

The two streams are deliberately **not** pre-joined at the source. Correlating them is the
job of the pipeline, and it is the part that only a stream processor can do correctly.

An HTTP Sink V2 to webhook.site was prototyped and **abandoned**: the free tier caps at 50
requests, which is not a usable sink for a continuously running stream. It is not part of
the architecture.

---

## 3. Data flow

```
   datagen-price-changes              datagen-sales-events
             |                                  |
             v                                  v
     +----------------+                +----------------+
     | price-changes  |                | sales-events   |   Avro, Schema Registry
     |  (append-only) |                |  (append-only) |
     +----------------+                +----------------+
             |                                  |
             | [1][2]                           |
             v                                  |
     +----------------+                         |
     |  price_state   |  UPSERT, PRIMARY KEY    |
     |  (versioned)   |  = product_id           |
     +----------------+                         |
             |                                  |
             +-------------+     +--------------+
                           |     |
                           v     v
                  +-------------------------+
                  |      sales_priced       |  [3] TEMPORAL JOIN
                  |  FOR SYSTEM_TIME AS OF  |      price live at moment of sale
                  +-------------------------+
                                |
                                v
                  +-------------------------+
                  |    demand_response      |  [4] TUMBLE 1 MINUTE, per product
                  +-------------------------+
                        |                |
                        |                | [5] ML_DETECT_ANOMALIES_ROBUST
                        |                v
                        |      +-------------------------+
                        |      |     response_scored     |  multivariate, ROW(...)
                        |      +-------------------------+
                        |                |
                        |          +-----+-----+
                        |          |           |
                        | [9]      | [6]       | [7]   MATCH_RECOGNIZE
                        v          v           v
            +-------------------+  +----------------+  +-----------------+
            | response_verdicts |  | overshoot_     |  | undershoot_     |
            |                   |  |   alerts       |  |   alerts        |
            |   PROVISIONAL     |  |   CONFIRMED    |  |   CONFIRMED     |
            |   ~1-2 min        |  |   ~20-30 min   |  |   ~20-30 min    |
            +-------------------+  +----------------+  +-----------------+
                   TIER 1                        TIER 2
```

---

## 4. Flink statements

One SQL file per statement in `../flink/`, numbered in dependency order. Each
`CREATE TABLE` and its `INSERT INTO` are submitted as two separate statements — Confluent Cloud Flink rejects a combined DDL+DML submission.

### [1] / [2] `price_state` — make the price feed joinable

`01-price-state-ddl.sql`, `02-price-state-insert.sql`

Converts the append-only `price-changes` topic into an UPSERT table keyed by
`PRIMARY KEY (product_id) NOT ENFORCED`, with `DISTRIBUTED BY (product_id) INTO 3 BUCKETS`.

This exists because Confluent's documentation requires the right side of a temporal join
to be a **versioned / updating table with a primary key**. A Datagen topic is append-only,
so joining `sales-events` directly against `price-changes` fails. In upsert mode the bucket
key must equal the primary key, which is why `DISTRIBUTED BY` is mandatory rather than
optional.

The result is a time-versioned view of "what was this product's price at any given moment".

### [3] `sales_priced` — the temporal join

`03-sales-priced.sql`

```sql
FROM `sales-events` AS s
JOIN price_state FOR SYSTEM_TIME AS OF s.`$rowtime` AS p
  ON s.product_id = p.product_id
```

Values every sale at the price that was genuinely live at the instant of that sale, and
carries `cost_cents`, `change_reason` and `intended_lift` forward onto the sale row.
Computes `margin_cents` per sale.

**This has no batch equivalent.** A batch job joining sales against a current-price table
misattributes every sale that straddles a price change — and those are precisely the sales
that carry the signal. See section 6.

### [4] `demand_response` — the heartbeat

`04-demand-response.sql`

`TUMBLE` over `sale_ts` at `INTERVAL '1' MINUTE`, grouped by `product_id`. Emits one row
per product per minute: `units_sold`, `total_margin`, `avg_price`, `intended_lift`.

Everything downstream consumes this regular per-minute pulse rather than raw sales.

### [5] `response_scored` — multivariate anomaly detection

`05-response-scored.sql`

```sql
ML_DETECT_ANOMALIES_ROBUST(
  ROW(avg_price, CAST(units_sold AS DOUBLE), CAST(total_margin AS DOUBLE)),
  window_time,
  JSON_OBJECT('window' VALUE 20, 'threshold' VALUE 3.0)
) OVER (PARTITION BY product_id ORDER BY window_time)
```

A $12 price is normal. 300 units/minute is normal. Negative margin is survivable. All three
simultaneously is a pricing failure. Three separate univariate checks would each pass
individually; `ROW(...)` evaluates them jointly.

Built-in function — no `CREATE MODEL`, no external inference endpoint, no third-party
credentials. Requires a 20-window warm-up before it scores anything.

### [6] `overshoot_alerts` — CEP, confirmed overshoot

`06-overshoot-match-recognize.sql`

```sql
PATTERN (PRICE_DROP SURGE{2,}?) WITHIN INTERVAL '10' MINUTE
AFTER MATCH SKIP PAST LAST ROW
DEFINE
  PRICE_DROP AS PRICE_DROP.avg_price < 5000,
  SURGE      AS SURGE.is_anomaly = TRUE AND SURGE.total_margin < 0
```

Reads as: *a price drop, followed by at least two consecutive windows of anomalous
negative-margin selling, all within ten minutes.*

This is an **ordered sequence**, not a filter and not an aggregate. `SURGE{2,}?` demands
sustained behaviour, so a single noisy window cannot fire it. `WITHIN INTERVAL '10' MINUTE`
bounds causality — a surge an hour later is unrelated. `AFTER MATCH SKIP PAST LAST ROW`
produces one alert per incident rather than one per window.

### [7] `undershoot_alerts` — CEP, confirmed undershoot

`07-undershoot-match-recognize.sql`

```sql
PATTERN (PRICE_DROP FLAT{3,}?) WITHIN INTERVAL '15' MINUTE
DEFINE
  PRICE_DROP AS PRICE_DROP.avg_price < 20000,
  FLAT       AS FLAT.units_sold <= 3 AND FLAT.is_anomaly = FALSE
```

The same machinery with the opposite sign: a price drop followed by three or more windows
in which demand did **not** respond. That both directions of the error signal fall out of
one mechanism is the argument for this being one application rather than two.

### [9] `response_verdicts` — the headline output

`09-response-verdicts.sql`

The control loop at single-window latency. Computes a rolling 10-window baseline per
product, derives `observed_lift` as a percentage against that baseline, and compares it
with the `intended_lift` recorded at price-change time.

| Condition | Verdict |
|---|---|
| `observed_lift > intended_lift * 2` | `OVERSHOOT` |
| `observed_lift < intended_lift / 2` | `UNDERSHOOT` |
| otherwise | `HEALTHY` |

Every row is stamped `confidence = 'PROVISIONAL'`.

**Verified rows from the live cluster**
(`product_id, verdict, intended_lift, observed_lift, units_sold, total_margin`):

```
["SKU-1001", "OVERSHOOT",  41, 97, 340, 5454908]
["SKU-1004", "HEALTHY",    47, 74, 264, 2927215]
["SKU-1009", "UNDERSHOOT", 57,  0,  97,  302027]
```

Row 1 reads: *we intended a 41% lift on SKU-1001 and got 97% — more than double intent.
That is an overshoot; margin was left on the table.*

### Not in the pipeline: `price_violations`

An earlier statement checked prices for structural legality (zero, one cent, above
$1000, below cost). It read **only** `price-changes` and never touched `sales-events`,
so it could not know whether a price *worked* — only whether it was *legal*. Measured
against the thesis, it was not part of the loop at all. It has been removed.

---

## 5. The two-tier design

The same question is answered twice, at two confidence levels, on two clocks.

| | Tier 1 — `response_verdicts` [9] | Tier 2 — `overshoot_alerts` / `undershoot_alerts` [6][7] |
|---|---|---|
| Evidence | ONE window | SUSTAINED sequence |
| Latency | ~1–2 min | ~20–30 min |
| Confidence | `PROVISIONAL` | `CONFIRMED` |
| Mechanism | Windowed compare against declared intent | `MATCH_RECOGNIZE` CEP over anomaly scores |
| False positives | Some | Very few |

A control loop that waits for certainty has already missed the decision window. So the
system acts on the provisional verdict immediately and lets CEP confirm or retire it
later. This is how real control systems behave, and it is a stronger position than either
tier alone: Tier 1 without Tier 2 is twitchy, Tier 2 without Tier 1 is too slow to steer
with.

---

## 6. Why streaming, not batch

Three independent reasons. Any one of them would be sufficient.

**1. The temporal join has no batch equivalent.**
`FOR SYSTEM_TIME AS OF` values each sale at the price live at that sale's own event time.
A batch join against a current-price snapshot misattributes every sale that straddles a
price change. Those straddling sales are exactly the ones that carry the demand-response
signal, so the error is not uniform noise — it is concentrated precisely where the answer
lives.

**2. The evidence is an ordered sequence.**
`MATCH_RECOGNIZE` asks for *a price drop, then two consecutive anomalous windows, within
ten minutes*. Expressed over a table this becomes self-joins plus correlated subqueries,
and it would still be wrong at window boundaries. Sequence and bounded causality are
first-class in the stream model and bolted-on in the batch model.

**3. Latency is the requirement, not a nice-to-have.**
Feedback must arrive faster than the system drifts. A verdict that shows up in tomorrow's
report describes a decision that was already made today, wrongly. Tier 1 lands in ~1–2
minutes, which is inside the operator's next-decision window.

---

## 7. Governance position

Stream Governance stays on the **Essentials** package. This is a deliberate decision, not
a budget default.

Essentials already provides everything the architecture actually uses:

- Schema Registry with Avro on every topic
- Auto-registered subjects from the Datagen connectors
- Compatibility enforcement
- Stream Lineage (10-minute window)
- Stream Catalog tags
- Data Portal

The headline reason to upgrade to Advanced would be **Data Contract rules**. Per
Confluent's own documentation, schema rules **do not execute in Kafka Connect or in Flink
SQL** — they bind on Java producer/consumer clients. This application's entire write path
is a Kafka Connect source connector, and its entire processing path is Flink SQL. An
Advanced upgrade would therefore buy a feature that is structurally inert in this
architecture. Stating that is more honest than paying for a checkbox.

---

## 8. Observing the pipeline

Native options, in order of preference for a demo:

| Method | Good for |
|---|---|
| `confluent flink shell` | Canonical CLI path. Results stream into the terminal in a live-updating table. |
| Cloud UI → Flink → SQL Workspace | Best visual. Live result grid, shareable. |
| Cloud UI → Topics → Messages | Raw records landing in a topic. No SQL needed. |
| Data Portal | Browsing topics as governed data products. |

```bash
source scripts/confluent-env.sh
confluent flink shell \
  --compute-pool lfcp-125m5jj \
  --database lkc-pgw3j8k \
  --cloud aws --region us-east-1
```

For scripted verification there is `scripts/query.sh`, which drives the Flink SQL REST
API. It exists because `confluent flink shell` is interactive and cannot be driven from a
script, while `confluent flink statement create` only ever reports `RUNNING` and never
shows rows.

See `outputs-explained.md` for what each output means and how it was verified.

---

## 9. Constraints discovered by trial

Each of these cost real time and none of them is obvious from the documentation.

| Constraint | Detail |
|---|---|
| Reluctant quantifier required | `MATCH_RECOGNIZE` rejects a greedy quantifier as the last pattern element: *"Greedy quantifiers are not allowed as the last element of a Pattern yet."* Use `{2,}?`, not `{2,}`. Not documented. |
| DDL and DML must be separate | `CREATE TABLE` and `INSERT INTO` cannot be submitted in one Flink statement. `recreate-all.sh` splits each file at the `INSERT` keyword. |
| Event time only | Confluent Cloud Flink does not support processing time. Every `ORDER BY` in `MATCH_RECOGNIZE` must use an event-time attribute. |
| Temporal join needs a versioned right side | Hence `price_state`. An append-only topic cannot be the right side of `FOR SYSTEM_TIME AS OF`. |
| Upsert bucket key = primary key | `DISTRIBUTED BY (product_id)` is mandatory on `price_state`, not decorative. |
| HTTP Sink V2 topic routing | Requires `api1.topics` **in addition to** the connector-level `topics`. Omitting it fails validation. `behavior.on.error` must be lowercase `ignore` / `fail`. |
| Local Kafka consume fails on the corporate network | Broker port 9092 cannot traverse an HTTP proxy, so `confluent kafka topic consume` fails from a Walmart laptop. Flink runs server-side and is unaffected — which is why all verification goes through Flink SQL. |
| Confluent CLI needs a specific proxy | `proxy-intlho.wal-mart.com:8080` works; `sysproxy.wal-mart.com:8080` returns HTTP 407. See `scripts/confluent-env.sh`. |
