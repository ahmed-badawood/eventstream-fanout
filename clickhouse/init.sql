CREATE DATABASE IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.engagement_enriched;

CREATE TABLE analytics.engagement_enriched
(
  event_id           UInt64,
  content_id         UUID,
  user_id            UUID,
  event_type         LowCardinality(String),
  event_ts           DateTime64(3, 'UTC'),
  duration_ms        Nullable(Int32),
  engagement_seconds Nullable(Float64),
  engagement_pct     Nullable(Decimal(5,2)),
  device             Nullable(String),
  content_type       LowCardinality(Nullable(String)),
  length_seconds     Nullable(Int32),
  processed_ts       DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (content_id, event_ts);
