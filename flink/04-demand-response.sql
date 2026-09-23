-- [4] demand_response: one row per product per minute.
-- The heartbeat that the anomaly detector and pattern matchers consume.

CREATE TABLE demand_response (
  product_id     STRING,
  window_time    TIMESTAMP_LTZ(3),
  units_sold     BIGINT,
  total_margin   BIGINT,
  avg_price      DOUBLE,
  intended_lift  INT,
  WATERMARK FOR window_time AS window_time - INTERVAL '5' SECOND
);

INSERT INTO demand_response
SELECT
  product_id,
  window_time,
  SUM(CAST(units AS BIGINT))              AS units_sold,
  SUM(CAST(margin_cents AS BIGINT))       AS total_margin,
  AVG(CAST(paid_price AS DOUBLE))         AS avg_price,
  MAX(intended_lift)                      AS intended_lift
FROM TABLE(TUMBLE(TABLE sales_priced, DESCRIPTOR(sale_ts), INTERVAL '1' MINUTE))
GROUP BY product_id, window_start, window_end, window_time;
