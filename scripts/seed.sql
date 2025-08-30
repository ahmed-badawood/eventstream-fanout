INSERT INTO content(id, slug, title, content_type, length_seconds, publish_ts)
VALUES ('00000000-0000-0000-0000-000000000001','hello','Hello','podcast',1800, now())
ON CONFLICT (id) DO NOTHING;

INSERT INTO engagement_events(content_id,user_id,event_type,event_ts,duration_ms,device,raw_payload)
VALUES
('00000000-0000-0000-0000-000000000001','u-1','play',   now(),  42000,'web','{}'),
('00000000-0000-0000-0000-000000000001','u-2','finish', now(), 180000,'ios','{}');
