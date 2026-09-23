-- [7] undershoot_alerts: CONFIRMED undershoot - the OTHER direction of the same signal.
--
-- "A price drop, followed by three or more windows where demand did NOT respond."
-- Same machinery as [6], opposite sign - which is the whole argument for overshoot and
-- undershoot being one application rather than two.
--
-- Same reluctant-quantifier constraint as [6]: {3,}? not {3,}.
--
-- FLAT is deliberately defined on is_anomaly = FALSE. That is the point: the failure
-- mode here is not a statistical outlier, it is the ABSENCE of the response we paid for.
-- Nothing looks wrong. Demand simply did not move, and the inventory will not clear.
--
-- Thresholds are measured, not guessed. Querying response_scored on the live stream
-- gave units_sold 2..502 and avg_price 14428..34528. An earlier version used
-- `units_sold <= 10`, which sat in the extreme bottom tail and essentially never
-- matched three times in a row. 200 is a genuine low-velocity window for this data.

CREATE TABLE undershoot_alerts (
  product_id        STRING,
  verdict           STRING,
  confidence        STRING,
  flat_start        TIMESTAMP_LTZ(3),
  flat_end          TIMESTAMP_LTZ(3),
  observed_velocity BIGINT,
  intended_lift     INT,
  avg_price         DOUBLE
);

INSERT INTO undershoot_alerts
SELECT
  product_id,
  'UNDERSHOOT' AS verdict,
  'CONFIRMED'  AS confidence,
  flat_start, flat_end, observed_velocity, intended_lift, flat_price
FROM response_scored
MATCH_RECOGNIZE (
  PARTITION BY product_id
  ORDER BY window_time
  MEASURES
    FIRST(FLAT.window_time) AS flat_start,
    LAST(FLAT.window_time)  AS flat_end,
    SUM(FLAT.units_sold)    AS observed_velocity,
    MAX(FLAT.intended_lift) AS intended_lift,
    AVG(FLAT.avg_price)     AS flat_price
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (PRICE_DROP FLAT{3,}?) WITHIN INTERVAL '15' MINUTE
  DEFINE
    PRICE_DROP AS PRICE_DROP.avg_price < 30000,
    FLAT       AS FLAT.units_sold <= 200 AND FLAT.is_anomaly = FALSE
) AS T;
