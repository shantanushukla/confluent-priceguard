# Submission Answers — PriceGuard

Copy-paste ready. Trim to the form's word limits.
Factual source of truth: `architecture.md`. Verified output: `outputs-explained.md`.

---

## Q1. Describe your Confluent application

### Version 1 — Full (~300 words)

> **PriceGuard is a markdown control loop. It answers, roughly ninety seconds after a price
> change lands, whether that change produced the demand response the operator intended.**
>
> A price change isn't a fact. It's a stimulus in a feedback loop. You change a price
> expecting a demand response. The response arrives over the next minutes. Your next
> decision depends on whether it matched intent. That's a control loop — and control loops
> require feedback arriving faster than the system drifts. Retail closes that loop in a
> nightly batch report, which is to say it does not close it at all.
>
> Two **Datagen Source connectors**, each driven by a custom Avro schema, produce an
> independent price feed and sales feed, correlated only by event time. Crucially the price
> feed carries `intended_lift_pct` — the operator's declared bet — so the pipeline compares
> intent against outcome instead of guessing a threshold.
>
> **Flink SQL** then closes the loop. An UPSERT table turns the append-only price feed into
> a versioned table so it can be the right side of a **temporal join**; `FOR SYSTEM_TIME AS
> OF` values every sale at the price that was live at the instant of that sale. One-minute
> tumbling windows produce a per-product demand pulse.
> `ML_DETECT_ANOMALIES_ROBUST` scores price, velocity and margin **jointly** — each is
> individually normal, all three at once is a pricing failure. `MATCH_RECOGNIZE` then
> matches an ordered sequence: a price drop followed by sustained anomalous negative-margin
> selling, within ten minutes.
>
> The output is a verdict per product per minute — OVERSHOOT, UNDERSHOOT or HEALTHY —
> naming the gap between intended and observed lift.
>
> **Who it's for:** pricing and revenue teams who own margin; e-commerce operations;
> merchandising planners deciding what to mark down next.
>
> **The benefit:** markdown decisions stop being open-loop guesses. A price change that
> overshot by 2x is visible in minutes, while the next decision is still in front of you.

### Version 2 — Short (~150 words)

> **PriceGuard is a markdown control loop on Confluent Cloud. It tells you, about ninety
> seconds after a price change, whether it produced the demand response you intended.**
>
> A price change isn't a fact — it's a stimulus in a feedback loop. Retail closes that loop
> in a nightly batch report, which is to say it does not close it.
>
> Two **Datagen Source connectors** with custom Avro schemas emit an independent price feed
> and sales feed. The price feed carries the operator's declared `intended_lift_pct`.
> **Flink SQL** temporal-joins every sale to the price live at that instant, aggregates a
> per-minute demand pulse, scores price, velocity and margin jointly with
> `ML_DETECT_ANOMALIES_ROBUST`, and matches sustained failure sequences with
> `MATCH_RECOGNIZE`.
>
> Output: OVERSHOOT / UNDERSHOOT / HEALTHY per product per minute, quantifying the gap
> between intent and outcome.
>
> **For:** pricing teams, e-commerce ops, merchandising planners.

### The line to lead with verbally

> "We intended a 41% lift on this SKU. We got 97%. That is not a success — that is a
> markdown that gave away twice the margin it needed to, and we know it in ninety seconds
> rather than tomorrow morning."

---

## Q2. Which Confluent connector(s) are you using?

### The answer

> **Datagen Source Connector for Confluent Cloud** (`connector.class: DatagenSource`) — two
> instances, each configured with a **custom Avro schema** via `schema.string` rather than a
> built-in quickstart template.

| Instance | Topic | Interval |
|---|---|---|
| `datagen-price-changes` | `price-changes` | 3000 ms |
| `datagen-sales-events` | `sales-events` | 300 ms |

Exact catalog name: **`Datagen Source (development and testing)`**.

### The justification

> Datagen is usually demoed with its built-in quickstart templates, which emit uniformly
> random values. That is useless here: a random price stream contains no *intent*, so there
> is nothing to measure an outcome against. Instead we supply a custom JSON-encoded Avro
> schema through `schema.string`, which lets us define both the shape of the data and the
> distribution of every field.
>
> The price schema models a real price feed — `product_id`, `category`, `old_price_cents`,
> `new_price_cents`, `cost_cents`, `change_reason`, `changed_by` — plus the field the whole
> application turns on: **`intended_lift_pct`**, the expected demand increase from this
> change. That single field is what converts an anomaly detector into a control loop. Without
> it the system can only say "this looks unusual"; with it the system can say "this is 2.4x
> what you asked for".
>
> We run **two** instances deliberately, and we do **not** pre-join them. The price feed and
> the sales feed are produced independently and correlated only by event time — exactly as
> they would be in a real retailer, where the pricing service and the order service are
> different systems. Correlating them correctly is the temporal join, and it is the part of
> the problem only a stream processor solves.

### Connector configuration (actual artifact)

```json
{
  "connector.class": "DatagenSource",
  "name": "datagen-price-changes",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.topic": "price-changes",
  "output.data.format": "AVRO",
  "schema.string": "<contents of schemas/price-changes.datagen.avsc, minified>",
  "max.interval": "3000",
  "tasks.max": "1"
}
```

Notes if asked:

- `schema.string` and the template option are **mutually exclusive**.
- The schema string is capped at **10,000 characters**; ours is ~2,200 minified.
- Two documented Cloud limitations we work around: the `regex` annotation is **not
  supported**, and `options` must be a JSON array, not a file reference.

### On sinks

An HTTP Sink V2 to webhook.site was prototyped to put an outbound arrow on the lineage
graph. It was **abandoned**: the free tier caps at 50 requests, which is not a sink for a
continuously running stream. Two documented gotchas from the attempt are worth keeping —
HTTP Sink V2 requires `api1.topics` **in addition to** the connector-level `topics`, and
`behavior.on.error` must be lowercase `ignore` / `fail`.

The application is complete without it: connector in, Flink processing, governed topics
out, consumed natively through the Flink shell, SQL Workspace and Data Portal.

---

## Supporting answers

### Stream processing

The pipeline is a chain of Flink SQL statements, each submitted as separate DDL and DML
because Confluent Cloud Flink rejects a combined submission.

| # | Statement | Technique | Why it exists |
|---|---|---|---|
| 1/2 | `price_state` | UPSERT table, `PRIMARY KEY`, `DISTRIBUTED BY` | The right side of a temporal join must be a versioned/updating table. An append-only Datagen topic is not. |
| 3 | `sales_priced` | `FOR SYSTEM_TIME AS OF` | Values every sale at the price live at the moment of sale. No batch equivalent. |
| 4 | `demand_response` | `TUMBLE` 1 minute | Per-product demand pulse. |
| 5 | `response_scored` | `ML_DETECT_ANOMALIES_ROBUST(ROW(...))` | Multivariate — price, velocity and margin scored jointly, not three univariate checks. |
| 6 | `overshoot_alerts` | `MATCH_RECOGNIZE`, `(PRICE_DROP SURGE{2,}?) WITHIN 10 MINUTE` | Confirms a sustained overshoot as an ordered sequence. |
| 7 | `undershoot_alerts` | `MATCH_RECOGNIZE` | Same machinery, opposite sign. |
| 9 | `response_verdicts` | Windowed compare vs `intended_lift` | The headline output. OVERSHOOT / UNDERSHOOT / HEALTHY at `PROVISIONAL` confidence. |

All ML functions are **built-in** — no `CREATE MODEL`, no external inference endpoint, no
third-party credentials. The whole application is reproducible by anyone with a Confluent
account.

### The two-tier answer (worth stating explicitly)

| | `response_verdicts` | `MATCH_RECOGNIZE` alerts |
|---|---|---|
| Evidence | ONE window | SUSTAINED sequence |
| Latency | ~1–2 min | ~20–30 min |
| Confidence | `PROVISIONAL` | `CONFIRMED` |

> A control loop that waits for certainty has already missed the decision window. We act on
> the provisional verdict and let CEP confirm or retire it later.

### Verified output

```
["SKU-1001", "OVERSHOOT",  41, 97, 340, 5454908]
["SKU-1004", "HEALTHY",    47, 74, 264, 2927215]
["SKU-1009", "UNDERSHOOT", 57,  0,  97,  302027]
```

The `MATCH_RECOGNIZE` alert tables are currently empty. Both statements are RUNNING and
correct; the pattern requires a price drop plus 2+ consecutive anomalous negative-margin
windows on top of a 20-window detector warm-up. Emptiness is the expected behaviour of a
high-precision detector on a short run.

### Stream governance

Running on the **Essentials** package:

- **Schema Registry** — Avro on every topic, subjects auto-registered by the connectors,
  compatibility enforced.
- **Stream Lineage** — end-to-end graph, 10-minute window.
- **Stream Catalog** — topic tags.
- **Data Portal** — topics browsable as governed data products.

**Why not Advanced:** the headline Advanced feature is Data Contract rules. Confluent's own
documentation confirms schema rules do **not** execute in Kafka Connect or Flink SQL — they
bind on Java producer/consumer clients. This application's write path is entirely a Connect
source connector and its processing path is entirely Flink SQL, so an Advanced upgrade
would buy a feature that is structurally inert in this architecture. We would rather name
that than pay for a checkbox.
