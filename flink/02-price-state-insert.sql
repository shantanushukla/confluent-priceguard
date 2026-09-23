-- [1b] Populate price_state from the price-changes stream.
-- Each new price for a product_id overwrites the previous one, giving Flink a
-- time-versioned view of "what was the price at any given moment".

INSERT INTO price_state
SELECT
  product_id,
  new_price_cents,
  cost_cents,
  change_reason,
  intended_lift_pct
FROM `price-changes`;
