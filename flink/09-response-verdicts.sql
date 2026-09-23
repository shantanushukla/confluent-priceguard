-- [9] response_verdicts: THE CONTROL LOOP, at single-window latency.
--
-- Answers the thesis question every minute, per product: did this price change produce
-- the demand response we intended? It compares OBSERVED lift against the INTENDED lift
-- that was recorded at the moment of the change.
--
-- Relationship to the MATCH_RECOGNIZE statements - same question, two confidence levels:
--   [9] this one            -> PROVISIONAL verdict from ONE window of feedback  (~1-2 min)
--   [6]/[7] MATCH_RECOGNIZE -> CONFIRMED verdict from a SUSTAINED sequence     (~20-30 min)
-- A control loop that waits for certainty has already missed the decision window.

CREATE TABLE response_verdicts (
  product_id       STRING,
  window_time      TIMESTAMP_LTZ(3),
  verdict          STRING,
  confidence       STRING,
  intended_lift    INT,
  observed_lift    INT,
  units_sold       BIGINT,
  baseline_units   DOUBLE,
  total_margin     BIGINT
);

INSERT INTO response_verdicts
SELECT
  product_id,
  window_time,
  -- WARMING_UP must be the FIRST branch.
  --
  -- BUG THIS FIXES: without it, the first windows for every product report
  -- observed_lift = 0, because the rolling baseline has no history yet - it equals the
  -- current value, so the lift ratio collapses to exactly zero. Those were then misread
  -- as genuine UNDERSHOOT. First run produced 670 false UNDERSHOOTs against 162 HEALTHY.
  -- Requiring 3 prior windows before judging removes the artifact entirely.
  CASE
    WHEN prior_windows < 3                            THEN 'WARMING_UP'
    WHEN observed_lift > intended_lift * 2            THEN 'OVERSHOOT'
    WHEN observed_lift < intended_lift / 2            THEN 'UNDERSHOOT'
    ELSE 'HEALTHY'
  END AS verdict,
  'PROVISIONAL' AS confidence,
  intended_lift,
  observed_lift,
  units_sold,
  baseline_units,
  total_margin
FROM (
  SELECT
    product_id, window_time, units_sold, total_margin, intended_lift,
    baseline_units, prior_windows,
    CAST(
      CASE WHEN baseline_units > 0
           THEN ((CAST(units_sold AS DOUBLE) - baseline_units) / baseline_units) * 100.0
           ELSE 0.0 END
      AS INT) AS observed_lift
  FROM (
    SELECT
      product_id, window_time, units_sold, total_margin, intended_lift,
      AVG(CAST(units_sold AS DOUBLE)) OVER w AS baseline_units,
      COUNT(*) OVER w                        AS prior_windows
    FROM demand_response
    WINDOW w AS (
      PARTITION BY product_id ORDER BY window_time
      ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    )
  )
);
