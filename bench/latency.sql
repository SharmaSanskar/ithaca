-- Exact latency percentiles for deliveries of events created in a window,
-- computed from the attempt log (not from histogram buckets, which can only
-- interpolate between bucket bounds).
--
--   queue_wait  event ingested  → first attempt starts   (time spent queued)
--   delivery    the successful attempt's HTTP duration    (the subscriber)
--   end_to_end  event ingested  → successful attempt ends
--
-- psql -v from=... -v to=... [-v endpoint=...]
WITH ev AS (
    SELECT id, created_at FROM events WHERE created_at >= :'from'::timestamptz AND created_at < :'to'::timestamptz
),
d AS (
    SELECT h.id, h.endpoint_id, ev.created_at
    FROM deliveries_history h JOIN ev ON ev.id = h.event_id
    WHERE h.final_state = 'succeeded'
),
first_attempt AS (
    SELECT DISTINCT ON (a.delivery_id) a.delivery_id, a.started_at
    FROM delivery_attempts a JOIN d ON d.id = a.delivery_id
    ORDER BY a.delivery_id, a.attempt, a.started_at
),
success AS (
    SELECT a.delivery_id, a.started_at, a.duration_ms
    FROM delivery_attempts a JOIN d ON d.id = a.delivery_id
    WHERE a.outcome = 'success'
),
m AS (
    SELECT e.description AS endpoint,
           EXTRACT(EPOCH FROM f.started_at - d.created_at) * 1000 AS queue_wait,
           s.duration_ms::float8 AS delivery,
           EXTRACT(EPOCH FROM s.started_at - d.created_at) * 1000 + s.duration_ms AS e2e
    FROM d
    JOIN first_attempt f ON f.delivery_id = d.id
    JOIN success s ON s.delivery_id = d.id
    JOIN endpoints e ON e.id = d.endpoint_id
)
SELECT endpoint, count(*) AS n,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY queue_wait)::numeric, 1) AS qw_p50,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY queue_wait)::numeric, 1) AS qw_p99,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY delivery)::numeric, 1)   AS del_p50,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY delivery)::numeric, 1)   AS del_p99,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY e2e)::numeric, 1)        AS e2e_p50,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e)::numeric, 1)        AS e2e_p95,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY e2e)::numeric, 1)        AS e2e_p99,
       round(max(e2e)::numeric, 1)                                                 AS e2e_max
FROM m
GROUP BY ROLLUP (endpoint)
ORDER BY endpoint NULLS LAST;
