-- name: CreateTenant :one
INSERT INTO tenants (id, name)
VALUES (@id, @name)
RETURNING *;

-- name: CreateAPIKey :one
INSERT INTO api_keys (id, tenant_id, key_prefix, key_hash, name)
VALUES (@id, @tenant_id, @key_prefix, @key_hash, @name)
RETURNING id, tenant_id, key_prefix, name, created_at;

-- name: GetAPIKeyByPrefix :one
-- Lookup is by the public prefix only. The caller compares key_hash in
-- constant time; the database never sees the secret.
SELECT k.id, k.tenant_id, k.key_hash, k.revoked_at, t.state AS tenant_state
FROM api_keys k
JOIN tenants t ON t.id = k.tenant_id
WHERE k.key_prefix = @key_prefix;
