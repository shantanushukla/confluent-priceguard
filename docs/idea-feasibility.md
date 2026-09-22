# Idea Feasibility — Confluent DSP Submission

> **Account verified 2026-09-22 via CLI** (logged in through `proxy-intlho` — see `scripts/confluent-env.sh`)

---

## 0. Live account state

| Item | Value | Impact on our plan |
|---|---|---|
| Org | `Walmart` (`5c3406eb-4004-4fef-b3ef-1e91a4fb8ff7`) | Real org, not personal trial |
| Environment | `env-d5zq11` "default" | Usable as-is |
| **Stream Governance** | **ESSENTIALS** |  **Blocks Data Contracts** — see §1 |
| Kafka clusters | none | Clean slate |
| Flink pools | none | Clean slate |
| Flink regions | `us-central1`, `us-south1`, `us-west4`, +global | Plenty |

###  The one finding that changes the plan

The environment is on **ESSENTIALS**, not Advanced. Per Confluent's package docs:

| Capability | Essentials | Advanced |
|---|---|---|
| Schema Registry |  |  |
| **Schema rules / Data Contracts** |  |  |
| Stream Lineage | 10 min only | 10 min + 7-day point-in-time |
| Stream Catalog tags |  |  |
| Business metadata |  |  |

**Consequence:** the "CEL rule rejects a bad price at serialization" story from the earlier
proposal is **not available on this environment today**. Three ways forward:

1. **Upgrade the env to Advanced** — `confluent schema-registry cluster upgrade --package advanced`
   (has a cost; may need approval on a Walmart-owned org).
2. **Personal free-tier account** (`cnfl.io/devday2026`, $400 credit) — full control, and the
   $400 comfortably covers Advanced for a demo. Also sidesteps corporate-org governance.
3. **Reframe governance around what Essentials does give us** — Schema Registry + Avro +
   compatibility enforcement + Catalog tags + Data Portal + 10-min lineage. Still a legitimate
   "Stream Governance" answer, just without the Data Contracts headline.

**Recommendation: option 2.** It's a submission demo, not production.

### Minor operational note
First API call through the corporate proxy often times out (~20s cold start), then works —
a retry loop showed 3/3 OK. Don't panic on a single timeout.

---

## 1. Feasibility matrix

Scored against what the submission actually grades: **business impact**, **connector use**,
**stream processing**, **stream governance**, plus can-we-actually-build-it.

| # | Idea | Impact | Demo ease | Build effort | Lineage richness | Originality | **Verdict** |
|---|---|---|---|---|---|---|---|
| **A** | **PriceGuard** — real-time price integrity firewall |  |  | Low |  |  | **BUILD** |
| **B** | **Dead Stock Radar** — forecast-driven clearance |  |  | Low |  |  | **BUILD (bolt-on)** |
| C | **FlowWatch** — stuck-order detection |  |  | Lowest |  |  | Fallback |
| D | **TrustStream** — PII-safe customer 360 |  |  | Low |  |  | Fold into A/B |
| E | **Card Fraud Velocity** |  |  | Low |  |  | Skip |

---

## 2. The shortlist, in one paragraph each

### A — PriceGuard ⭐ primary

> Catches wrong prices in the seconds **before** they reach the shelf edge, instead of in next
> week's revenue report.

Everyone instantly understands glitch pricing: a decimal slips, a $99 item sells for $0.99,
thousands of times, at machine speed, before a human notices. Zero background required — yet
it is precisely the generic form of what omni-clearance exists to prevent, so we can speak
about it from real experience.

- **Data:** Datagen `SHOES` (`sale_price`) + `SHOE_ORDERS` + `INVENTORY` — all verified real templates.
- **Killer technical moment:** `ML_DETECT_ANOMALIES_ROBUST(ROW(sale_price, order_velocity, margin_pct), ...)`.
  A $12 shoe is fine. A $12 shoe suddenly selling 300/min is not. **Univariate detection misses
  this; multivariate catches it** — that contrast is the thing to demo.
- **Feasible today?** Yes, fully. Zero `CREATE MODEL`, no external endpoints, no credentials.
- **Risk:** the governance headline weakens on Essentials (see §0).

### B — Dead Stock Radar ⭐ bolt-on

> Predicts which products will still be sitting in the warehouse in three weeks, and flags them
> for markdown while the discount still needs to be small.

The margin argument is unusually clean: acting 30 days earlier at 20% off beats acting late at
70% off, and it frees working capital sooner. One decision, two wins.

- **Data:** same three topics as A — **this is why it's nearly free to add.**
- **Technical:** `ML_FORECAST` (built-in ARIMA) on sell-through velocity, then
  `days_to_clear = quantity / forecast_velocity`.
- **Feasible today?** Yes. One caveat: ARIMA needs `minTrainingSize` windows before it emits
  anything. Use short windows or pre-seed history so it warms up inside the judging window.

### Why A+B together, not A alone

- **Same three Datagen sources** → one set of connectors, two headline features.
- Covers **both built-in ML families** — anomaly *and* forecast — with zero model registration.
- Produces a **visually rich Stream Lineage graph**: 3 sources → several Flink statements → 3
  sinks. Lineage is a graded field; a thin pipeline screenshots badly.

### C — FlowWatch (fallback)

Uses the ready-made `PIZZA_ORDERS` / `_COMPLETED` / `_CANCELLED` trio, so stuck-order detection
needs no synthetic gap injection — genuinely the easiest build. Docked only because the impact
story is operational rather than financial, which scores lower on "most business impact."

### D — TrustStream (fold in, don't build standalone)

`AI_DETECT_PII` redaction is a strong governance beat, but as a standalone app it *prevents a
cost* rather than creating value. Better used as one Flink statement inside A/B —
especially valuable now that Data Contracts are off the table on Essentials.

### E — Card Fraud (skip)

Good idea, fatal problem: it is **the** most common Confluent demo in existence. Against a field
of submissions it's the expected answer, and we'd have no real domain experience behind it.

---

## 3. Proposed build

```
Datagen SHOES        ─┐                              ┌─ price_violations   → HTTP Sink V2
Datagen SHOE_ORDERS  ─┼─▶ Flink SQL ─────────────────┼─ clearance_candidates → BigQuery Sink V2
Datagen INVENTORY    ─┘   · ML_DETECT_ANOMALIES_ROBUST└─ velocity_metrics   → GCS Sink
                          · ML_FORECAST
                          · AI_DETECT_PII
```

**Connectors (4):** `Datagen Source (development and testing)` ×3, `Google BigQuery Sink V2`
*(V2 — the original is End-of-Life)*, `Google Cloud Storage Sink`, `HTTP Sink V2`.

**Known limitation to state in the submission:** Confluent docs confirm schema rules do **not**
execute in Kafka Connect or Flink SQL — they bind on Java clients. For Datagen-sourced records
the equivalent is a Flink filter into a DLQ topic. Better named up front than found in Q&A.

---

## 4. Decisions needed from you

1. **Governance package** — upgrade `env-d5zq11` to Advanced, spin up a personal free-tier
   account, or reframe around Essentials? *(recommend: personal account)*
2. **Confirm A+B** as the build.
3. **Cloud/region** — GCP `us-central1` suggested (Flink-supported, matches the GCS/BigQuery sinks).
4. **Do the GCP sinks need real GCP creds?** If not readily available, swap BigQuery/GCS for a
   second HTTP Sink or drop to one sink — the lineage graph survives either way.
