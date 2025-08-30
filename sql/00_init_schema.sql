CREATE TABLE IF NOT EXISTS content(
  id UUID PRIMARY KEY,
  slug TEXT UNIQUE NOT NULL,
  title TEXT NOT NULL,
  content_type TEXT CHECK (content_type IN ('podcast','newsletter','video')),
  length_seconds INTEGER,
  publish_ts TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS engagement_events(
  id BIGSERIAL PRIMARY KEY,
  content_id UUID REFERENCES content(id),
  user_id UUID,
  event_type TEXT CHECK (event_type IN ('play','pause','finish','click')),
  event_ts TIMESTAMPTZ NOT NULL,
  duration_ms INTEGER,
  device TEXT,
  raw_payload JSONB
);
INSERT INTO content VALUES
('00000000-0000-0000-0000-000000000001','how-to-start','How to Start','podcast',1800, now())
ON CONFLICT (id) DO NOTHING;
