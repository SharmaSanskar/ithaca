/**
 * REST layer: types for the dashboard API and TanStack Query hooks.
 *
 * The architectural spine of the dashboard is a split of responsibilities:
 *
 *   TanStack Query owns ENTITY state (endpoints, events, dead letters) and
 *   never sees the firehose.
 *   The StreamStore (stream.ts) owns STREAM state (10Hz frames) and never sets
 *   React state per message.
 *
 * WebSocket deltas cross from one to the other through setQueryData with a
 * revision guard (see live.ts) — they patch cached entities, never insert or
 * reorder rows, which would corrupt keyset pagination.
 */
import {
  keepPreviousData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'

// ── types (mirror internal/api and internal/live) ───────────────────────────

export interface Endpoint {
  id: string
  name: string
  url: string
  state: string
  circuit: 'closed' | 'open' | 'half_open'
  consecutive_failures: number
  circuit_next_probe_at?: string
  depth: number
  rev: number
}

export interface Totals {
  pending: number
  retrying: number
  dlq: number
  paused_streams: number
}

export interface Overview {
  tenant: string
  endpoints: Endpoint[]
  totals: Totals
}

export interface EventRow {
  id: string
  type: string
  ordering_key?: string
  created_at: string
  pending: number
  succeeded: number
  dead: number
}

export interface Attempt {
  attempt: number
  started_at: string
  duration_ms: number | null
  outcome: 'success' | 'retryable' | 'timeout' | 'permanent' | 'rate_limited' | 'blocked'
  status_code?: number
  error_class?: string
  error?: string
}

export interface Delivery {
  id: string
  endpoint_id: string
  seq: number
  status: 'pending' | 'retrying' | 'succeeded' | 'dead'
  attempts: number
  next_attempt_at?: string
  completed_at?: string
  last_error?: string
  dead_reason?: string
  replay_of?: string
  history: Attempt[]
}

export interface EventDetail {
  id: string
  type: string
  ordering_key?: string
  created_at: string
  payload: unknown
  deliveries: Delivery[]
}

export interface DeadLetter {
  id: string
  event_id: string
  event_type: string
  endpoint_id: string
  attempts: number
  dead_reason: string | null
  last_error: string | null
  died_at: string
  replayed_at?: string
  replayed_as?: string
  replay_of?: string
  blocks_stream: boolean
}

export interface SeriesPoint {
  t: number
  ingested: number
  succeeded: number
  failed: number
  rate_limited: number
  p50_ms: number
  p99_ms: number
}

export interface SinkBehavior {
  status: number
  latency_ms: number
  fail_rate: number
  fail_status: number
}

export interface Scenario {
  name: string
  title: string
  description: string
}

export interface DemoState {
  leader: boolean
  scenario: { name: string; title: string; step: number; total: number; started_at: string } | null
  traffic_rate: number
  sinks: Record<string, SinkBehavior>
  scenarios: Scenario[]
}

interface Page<T> {
  data: T[]
  next_cursor?: string
}

// ── fetch ───────────────────────────────────────────────────────────────────

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message)
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api/v1${path}`, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...init?.headers },
  })
  if (!res.ok) {
    let code = 'http_error'
    let message = `${res.status} ${res.statusText}`
    try {
      const body = (await res.json()) as { error?: { code: string; message: string } }
      if (body.error) ({ code, message } = body.error)
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(res.status, code, message)
  }
  return (await res.json()) as T
}

// ── query keys: one place, so deltas know exactly what to patch ─────────────

export const qk = {
  overview: ['overview'] as const,
  events: ['events'] as const,
  event: (id: string) => ['event', id] as const,
  dlq: (endpoint?: string) => ['dlq', endpoint ?? 'all'] as const,
  series: (window: number, endpoint?: string) => ['series', window, endpoint ?? 'all'] as const,
  demo: ['demo'] as const,
}

// ── queries ─────────────────────────────────────────────────────────────────

export function useOverview() {
  return useQuery({ queryKey: qk.overview, queryFn: () => request<Overview>('/dash/overview') })
}

export function useEndpointsById() {
  const { data } = useOverview()
  const m = new Map<string, Endpoint>()
  for (const e of data?.endpoints ?? []) m.set(e.id, e)
  return m
}

export function useEvents() {
  return useInfiniteQuery({
    queryKey: qk.events,
    queryFn: ({ pageParam }) =>
      request<Page<EventRow>>(`/dash/events?limit=50${pageParam ? `&cursor=${pageParam}` : ''}`),
    initialPageParam: '',
    getNextPageParam: (last) => last.next_cursor,
    refetchInterval: 5_000,
  })
}

export function useEvent(id: string) {
  return useQuery({
    queryKey: qk.event(id),
    queryFn: () => request<EventDetail>(`/dash/events/${id}`),
    // An event still being delivered changes as attempts land; a finished
    // one does not.
    refetchInterval: (q) =>
      q.state.data?.deliveries.some((d) => d.status === 'pending' || d.status === 'retrying')
        ? 1_000
        : false,
  })
}

export function useDeadLetters(endpoint?: string) {
  return useInfiniteQuery({
    queryKey: qk.dlq(endpoint),
    queryFn: ({ pageParam }) =>
      request<Page<DeadLetter>>(
        `/dash/dlq?limit=100${endpoint ? `&endpoint_id=${endpoint}` : ''}${pageParam ? `&cursor=${pageParam}` : ''}`,
      ),
    initialPageParam: '',
    getNextPageParam: (last) => last.next_cursor,
    placeholderData: keepPreviousData,
  })
}

export function useSeries(windowSec = 900, endpoint?: string, bucket = 10) {
  return useQuery({
    queryKey: qk.series(windowSec, endpoint),
    queryFn: () =>
      request<{ bucket_seconds: number; data: SeriesPoint[] }>(
        `/dash/series?window=${windowSec}&bucket=${bucket}${endpoint ? `&endpoint_id=${endpoint}` : ''}`,
      ),
    refetchInterval: 10_000,
  })
}

export function useDemoState() {
  return useQuery({
    queryKey: qk.demo,
    queryFn: () => request<DemoState>('/demo/state'),
    refetchInterval: 2_000,
  })
}

// ── mutations ───────────────────────────────────────────────────────────────

export function useReplay() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (body: { ids?: string[]; endpoint_id?: string; all?: boolean }) =>
      request<{ replayed: number; resumed_streams: number; skipped: number }>('/dash/dlq/replay', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSettled: () => {
      void qc.invalidateQueries({ queryKey: ['dlq'] })
      void qc.invalidateQueries({ queryKey: qk.overview })
    },
  })
}

export function useDemoAction() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({
      path,
      method = 'POST',
      body,
    }: {
      path: string
      method?: string
      body?: unknown
    }) =>
      request<unknown>(`/demo${path}`, {
        method,
        body: body === undefined ? undefined : JSON.stringify(body),
      }),
    onSettled: () => void qc.invalidateQueries({ queryKey: qk.demo }),
  })
}
