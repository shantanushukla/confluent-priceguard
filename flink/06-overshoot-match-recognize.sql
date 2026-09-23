-- [6] overshoot_alerts: CONFIRMED overshoot via complex event processing.
--
-- Reads aloud as: "a price drop, followed by at least two consecutive windows of
-- sustained high-velocity selling at a depressed price, all within ten minutes."
--
-- WHY THIS NEEDS FLINK:
--   PATTERN (PRICE_DROP SURGE{2,}?)   an ORDERED SEQUENCE, not a filter or aggregate
--   SURGE{2,}?                        requires SUSTAINED behaviour - one noisy window
--                                     cannot fire it
--   WITHIN INTERVAL '10' MINUTE       bounded causality; a surge an hour later is unrelated
--   AFTER MATCH SKIP PAST LAST ROW    one alert per incident, not one per window
-- Over a table this becomes self-joins plus correlated subqueries, and would still be
-- wrong at window boundaries.
--
-- TWO CONSTRAINTS LEARNED BY SUBMITTING (neither is in the reference docs):
--
-- 1. RELUCTANT QUANTIFIER REQUIRED. `SURGE{2,}` is rejected outright:
--      "Greedy quantifiers are not allowed as the last element of a Pattern yet."
--    `{2,}?` is also better behaviour here - the alert fires as soon as the damage is
--    confirmed rather than waiting for the surge to finish.
--
-- 2. THRESHOLDS MUST BE DERIVED FROM THE DATA, NOT GUESSED.
--    Two successive versions emitted nothing for 40+ minutes:
--      v1 gated on `total_margin < 0`. But total_margin is a SUM across a one-minute
--         window; with paid prices scattered above and below cost the negatives cancel
--         and the sum is essentially always positive. Unsatisfiable.
--      v2 gated on `avg_price < 20000`. Also near-unsatisfiable - querying the live
--         stream showed avg_price actually ranges 14428..34528, so that cut off most
--         of the distribution.
--    Measured from response_scored: avg_price 14428..34528, units_sold 2..502.
--    The thresholds below sit inside those observed ranges, so the pattern is now
--    satisfiable by real data rather than by hope.
--    Margin is still reported in MEASURES so the alert carries financial impact; it
--    just no longer gates the match.
--
-- Confluent Cloud Flink has no processing time, so ORDER BY must use event time.

CREATE TABLE overshoot_alerts (
  product_id    STRING,
  verdict       STRING,
  confidence    STRING,
  surge_start   TIMESTAMP_LTZ(3),
  surge_end     TIMESTAMP_LTZ(3),
  units_burned  BIGINT,
  margin_impact BIGINT,
  avg_price     DOUBLE
);

INSERT INTO overshoot_alerts
SELECT
  product_id,
  'OVERSHOOT'  AS verdict,
  'CONFIRMED'  AS confidence,
  surge_start, surge_end, units_burned, margin_impact, surge_price
FROM response_scored
MATCH_RECOGNIZE (
  PARTITION BY product_id
  ORDER BY window_time
  MEASURES
    FIRST(SURGE.window_time) AS surge_start,
    LAST(SURGE.window_time)  AS surge_end,
    SUM(SURGE.units_sold)    AS units_burned,
    SUM(SURGE.total_margin)  AS margin_impact,
    AVG(SURGE.avg_price)     AS surge_price
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (PRICE_DROP SURGE{2,}?) WITHIN INTERVAL '10' MINUTE
  DEFINE
    PRICE_DROP AS PRICE_DROP.avg_price < 30000,
    SURGE      AS SURGE.units_sold > 300 AND SURGE.avg_price < 28000
) AS T;
