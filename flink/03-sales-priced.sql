-- [3] sales_priced: THE TEMPORAL JOIN.
--
-- Values every sale at the price that was genuinely live AT THE MOMENT OF SALE.
-- A batch job joining sales to a current-price table silently misattributes every
-- sale that straddles a price change - and those are exactly the sales we care about.
--
-- FOR SYSTEM_TIME AS OF has no batch equivalent. This is why the project needs Flink.

CREATE TABLE sales_priced (
  product_id     STRING,
  units          INT,
  paid_price     INT,
  cost_cents     INT,
  change_reason  STRING,
  intended_lift  INT,
  margin_cents   INT,
  sale_ts        TIMESTAMP_LTZ(3),
  WATERMARK FOR sale_ts AS sale_ts - INTERVAL '5' SECOND
);

INSERT INTO sales_priced
SELECT
  s.product_id,
  s.units,
  s.paid_price_cents,
  p.cost_cents,
  p.change_reason,
  p.intended_lift,
  (s.paid_price_cents - p.cost_cents) * s.units AS margin_cents,
  s.`$rowtime`
FROM `sales-events` AS s
JOIN price_state FOR SYSTEM_TIME AS OF s.`$rowtime` AS p
  ON s.product_id = p.product_id;
