# What Each Output Is, And How To See It

Companion to `architecture.md`. That document explains why the pipeline is shaped the way
it is; this one explains what comes out of it and how the claims were checked.

Verified 2026-09-23 against the live cluster.

---

## The native way to see output: `confluent flink shell`

```bash
source scripts/confluent-env.sh
confluent flink shell \
  --compute-pool lfcp-125m5jj \
  --database lkc-pgw3j8k \
  --cloud aws --region us-east-1
```

Then type SQL. Results stream into the terminal in a live-updating table. This is the
canonical Confluent path and the one to use in a demo.

Three other native options:

| Method | Good for |
|---|---|
| Cloud UI → Flink → SQL Workspace | Best visual. Live-updating result grid, shareable. |
| Cloud UI → Topics → Messages | Raw records landing in a topic. No SQL needed. |
| Data Portal | Browsing topics as governed data products. |

### Why `scripts/query.sh` also exists

`confluent flink shell` is interactive and cannot be driven from a script, and
`confluent flink statement create` only ever reports `RUNNING` — it never shows data. For
automated checks, `scripts/query.sh` drives the Flink SQL REST API directly:

```bash
./scripts/query.sh "SELECT * FROM response_verdicts LIMIT 10;"
```

Use the shell or the UI for demos; use this for scripted verification.

---

## The outputs

### `response_verdicts` — the headline. Working.

Every minute, for every product: **did the price change produce the demand response we
intended?** It compares `observed_lift` against the `intended_lift` recorded at the moment
of the change.

Verified rows from the live cluster
(`product_id, verdict, intended_lift, observed_lift, units_sold, total_margin`):

```
["SKU-1001", "OVERSHOOT",  41, 97, 340, 5454908]
["SKU-1004", "HEALTHY",    47, 74, 264, 2927215]
["SKU-1009", "UNDERSHOOT", 57,  0,  97,  302027]
```

Read row 1: *we intended a 41% lift on SKU-1001 and got 97% — more than double intent.
That is an overshoot; margin was left on the table.*

This is the control loop, and it is the output to lead with.

### `overshoot_alerts` / `undershoot_alerts` — currently empty. Verified.

```
SELECT COUNT(*) FROM overshoot_alerts;   -> (no rows)
SELECT COUNT(*) FROM undershoot_alerts;  -> (no rows)
```

Both statements are RUNNING and correct. They have simply never matched.
`MATCH_RECOGNIZE` demands a price drop followed by 2+ consecutive windows that are
simultaneously anomalous and negative-margin, inside 10 minutes — stacked on top of the
anomaly detector's 20-window warm-up.

These are the **confirmation** tier, not the detection tier. Emptiness is the expected
behaviour of a high-precision detector on a short run, not a defect.

### Intermediate tables worth showing

| Table | Why it is interesting on screen |
|---|---|
| `sales_priced` | The temporal join. Each sale carries the price that was live at its own event time. |
| `demand_response` | One row per product per minute. The heartbeat everything downstream consumes. |
| `response_scored` | `is_anomaly` from multivariate `ML_DETECT_ANOMALIES_ROBUST`. |

---

## How the two tiers relate

| | `response_verdicts` [9] | `overshoot_alerts` / `undershoot_alerts` [6][7] |
|---|---|---|
| Evidence | ONE window | SUSTAINED sequence |
| Latency | ~1–2 min | ~20–30 min |
| Confidence | `PROVISIONAL` | `CONFIRMED` |
| Mechanism | Windowed compare against declared intent | `MATCH_RECOGNIZE` CEP |
| False positives | Some | Very few |

A control loop that waits for certainty has already missed the decision window. Act on the
provisional verdict immediately; let CEP confirm or retire it later.

---

## Known bug: UNDERSHOOT is over-counting

```
UNDERSHOOT: 670    HEALTHY: 162    OVERSHOOT: (small)
```

Every UNDERSHOOT row has `observed_lift = 0`. That is not real undershoot. It is the
**first window for each product**, where the rolling baseline has no history, so
`baseline_units` equals the current value and lift computes to exactly zero.

The fix is to require a warm baseline before judging:

```sql
WHEN baseline_units < 1                THEN 'WARMING_UP'
WHEN observed_lift < intended_lift / 2 THEN 'UNDERSHOOT'
```

Not yet applied. Worth fixing before a demo, because "670 undershoots" is visibly wrong to
anyone who looks closely.

---

## Demo script

```bash
confluent flink shell --compute-pool lfcp-125m5jj --database lkc-pgw3j8k \
  --cloud aws --region us-east-1
```

```sql
-- 1. The thesis, live: intent versus outcome
SELECT product_id, verdict, intended_lift, observed_lift, units_sold, total_margin
FROM response_verdicts WHERE verdict <> 'HEALTHY';

-- 2. The temporal join: price as of sale time
SELECT * FROM sales_priced LIMIT 10;

-- 3. The per-minute heartbeat the detectors consume
SELECT * FROM demand_response LIMIT 10;
```
