-- name: CreateSubscription :one
INSERT INTO subscriptions (id, tenant_id, endpoint_id, event_types, ordered, lane_count)
VALUES (@id, @tenant_id, @endpoint_id, @event_types, @ordered, @lane_count)
RETURNING *;

-- name: GetSubscription :one
SELECT * FROM subscriptions
WHERE tenant_id = @tenant_id AND id = @id;

-- name: ListSubscriptions :many
SELECT * FROM subscriptions
WHERE tenant_id = @tenant_id
ORDER BY id
LIMIT 500;

-- name: SetSubscriptionState :one
UPDATE subscriptions
SET state = @state
WHERE tenant_id = @tenant_id AND id = @id
RETURNING *;

-- name: ListFanoutSubscriptions :many
-- Every live subscription for a tenant whose endpoint is accepting traffic.
-- ORDER BY s.id is load-bearing: ingest locks one stream row per subscription
-- in this order, and a consistent lock order across transactions is what makes
-- concurrent fanouts deadlock-free.
SELECT s.id, s.endpoint_id, s.event_types, s.ordered, s.lane_count
FROM subscriptions s
JOIN endpoints e ON e.id = s.endpoint_id
WHERE s.tenant_id = @tenant_id
  AND s.state = 'enabled'
  AND e.state = 'enabled'
ORDER BY s.id;
