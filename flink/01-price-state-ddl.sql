-- [1] price_state: convert the append-only price-changes feed into an UPSERT table.
--
-- WHY THIS EXISTS: Confluent docs state the right (versioned) side of a temporal join
-- "must be an updating table with a primary key". A Datagen topic is append-only, so
-- joining sales-events directly against price-changes FAILS. This statement is the
-- required bridge.
--
-- DISTRIBUTED BY is mandatory: in upsert mode the bucket key must equal the primary key.

CREATE TABLE price_state (
  product_id      STRING,
  current_price   INT,
  cost_cents      INT,
  change_reason   STRING,
  intended_lift   INT,
  PRIMARY KEY (product_id) NOT ENFORCED
) DISTRIBUTED BY (product_id) INTO 3 BUCKETS
  WITH ('changelog.mode' = 'upsert');
