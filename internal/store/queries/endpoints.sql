-- name: CreateEndpoint :one
INSERT INTO endpoints (id, tenant_id, url, description, timeout_ms, max_concurrency,
                       signing_key_id, signing_secret)
VALUES (@id, @tenant_id, @url, @description, @timeout_ms, @max_concurrency,
        @signing_key_id, @signing_secret)
RETURNING *;

-- name: GetEndpoint :one
SELECT * FROM endpoints
WHERE tenant_id = @tenant_id AND id = @id;

-- name: ListEndpoints :many
SELECT * FROM endpoints
WHERE tenant_id = @tenant_id
ORDER BY id
LIMIT 500;

-- name: SetEndpointState :one
UPDATE endpoints
SET state = @state, disabled_reason = sqlc.narg(disabled_reason), updated_at = now()
WHERE tenant_id = @tenant_id AND id = @id
RETURNING *;
