-- +goose Up
-- Phase 4: indexes for the live dashboard. Every query the hub runs once a
-- second, or the dashboard runs per page load, gets an index that matches it.

-- Per-endpoint backlog depth (hub, every second).
CREATE INDEX deliveries_endpoint_idx ON deliveries (endpoint_id);

-- Unreplayed dead letters per tenant (hub, every second). Partial: only the
-- live DLQ, which stays small even when history is huge.
CREATE INDEX dh_dlq_open_idx ON deliveries_history (tenant_id)
    WHERE final_state = 'dead' AND replayed_at IS NULL;

-- Throughput and latency series over recent attempts.
CREATE INDEX da_tenant_created_idx ON delivery_attempts (tenant_id, created_at DESC);

-- Delivery status per event (the event feed and event detail page).
CREATE INDEX dh_event_completed_idx ON deliveries_history (event_id, completed_at);

-- The demo tenant is looked up by name.
CREATE INDEX tenants_name_idx ON tenants (name);
