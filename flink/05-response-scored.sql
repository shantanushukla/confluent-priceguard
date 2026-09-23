-- [5] response_scored: MULTIVARIATE anomaly detection.
--
-- The point: a $12 price is normal. 300 units/min is normal. Negative margin is
-- survivable. ALL THREE AT ONCE is a pricing failure. Three separate univariate
-- checks would each pass. ROW(...) evaluates them together.

CREATE TABLE response_scored (
  product_id    STRING,
  window_time   TIMESTAMP_LTZ(3),
  units_sold    BIGINT,
  total_margin  BIGINT,
  avg_price     DOUBLE,
  intended_lift INT,
  is_anomaly    BOOLEAN,
  WATERMARK FOR window_time AS window_time - INTERVAL '5' SECOND
);

INSERT INTO response_scored
SELECT
  product_id, window_time, units_sold, total_margin, avg_price, intended_lift,
  anomaly.is_anomaly
FROM (
  SELECT
    product_id, window_time, units_sold, total_margin, avg_price, intended_lift,
    ML_DETECT_ANOMALIES_ROBUST(
      ROW(avg_price, CAST(units_sold AS DOUBLE), CAST(total_margin AS DOUBLE)),
      window_time,
      JSON_OBJECT('window' VALUE 20, 'threshold' VALUE 3.0)
    ) OVER (PARTITION BY product_id ORDER BY window_time) AS anomaly
  FROM demand_response
);
