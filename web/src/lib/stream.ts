/**
 * The StreamStore: the dashboard's real-time state, outside React.
 *
 * Frames arrive at 10Hz. Each one is folded into ring buffers and smoothed
 * rates, and a new immutable snapshot is published; React reads it through
 * useSyncExternalStore with a selector. Nothing per-message ever becomes React
 * state, and the canvas reads the store directly in its rAF loop.
 */
import { useSyncExternalStore } from 'react'

export interface FrameEndpoint {
  id: string
  name: string
  state: string
  circuit: 'closed' | 'open' | 'half_open'
  depth: number
  ok: number
  retry: number
  dead: number
  rate_limited: number
  p50_ms: number
  p99_ms: number
}

export interface Sample {
  event_id: string
  delivery_id?: string
  endpoint_id?: string
  type: string
  outcome: 'ingested' | 'delivered' | 'retry' | 'dead' | 'rate_limited'
  attempt?: number
  latency_ms?: number
  at: number
}

export interface Frame {
  type: 'frame'
  seq: number
  t: number
  window_ms: number
  ingested: number
  endpoints: FrameEndpoint[]
  totals: { pending: number; retrying: number; dlq: number; paused_streams: number }
  pool: { workers: number; busy: number; processes: number }
  samples: Sample[]
}

export interface Narration {
  scenario: string
  step: number
  total: number
  text: string
  level: 'info' | 'warn' | 'success'
  done?: boolean
  at: string
}

export type Connection = 'connecting' | 'live' | 'reconnecting'

/** Per-second rates, smoothed over the last second of frames. */
export interface Rates {
  ingested: number
  delivered: number
  retried: number
  dead: number
  rateLimited: number
  /** per endpoint id */
  endpoints: Record<
    string,
    {
      attempts: number
      ok: number
      retry: number
      dead: number
      rl: number
      p50: number
      p99: number
    }
  >
}

export interface Snapshot {
  connection: Connection
  frame: Frame | null
  rates: Rates
  /** last 60 one-second buckets, oldest first */
  history: {
    ingested: number[]
    delivered: number[]
    failed: number[]
    pending: number[]
    endpoints: Record<string, number[]>
  }
  feed: Sample[] // newest first
  narration: Narration[]
  superseded: number
}

const FEED_SIZE = 60
const HISTORY_SECONDS = 60
const WINDOW_FRAMES = 10 // 1 second at 10Hz

const emptyRates: Rates = {
  ingested: 0,
  delivered: 0,
  retried: 0,
  dead: 0,
  rateLimited: 0,
  endpoints: {},
}

class StreamStore {
  private listeners = new Set<() => void>()
  private recent: Frame[] = [] // last WINDOW_FRAMES frames
  private bucket = {
    t: 0,
    ingested: 0,
    delivered: 0,
    failed: 0,
    pending: 0,
    endpoints: {} as Record<string, number>,
  }
  private frameListeners = new Set<(f: Frame) => void>()

  snapshot: Snapshot = {
    connection: 'connecting',
    frame: null,
    rates: emptyRates,
    history: { ingested: [], delivered: [], failed: [], pending: [], endpoints: {} },
    feed: [],
    narration: [],
    superseded: 0,
  }

  subscribe = (fn: () => void) => {
    this.listeners.add(fn)
    return () => this.listeners.delete(fn)
  }

  /** Raw frame subscription, for the canvas engine (no React involved). */
  onFrame(fn: (f: Frame) => void) {
    this.frameListeners.add(fn)
    return () => this.frameListeners.delete(fn)
  }

  private publish(next: Partial<Snapshot>) {
    this.snapshot = { ...this.snapshot, ...next }
    for (const l of this.listeners) l()
  }

  setConnection(c: Connection) {
    if (c !== this.snapshot.connection) this.publish({ connection: c })
  }

  pushFrame(f: Frame) {
    for (const l of this.frameListeners) l(f)

    this.recent.push(f)
    if (this.recent.length > WINDOW_FRAMES) this.recent.shift()
    const rates = computeRates(this.recent)

    // One-second history buckets for sparklines and node footers.
    const sec = Math.floor(f.t / 1000)
    let history = this.snapshot.history
    if (this.bucket.t !== sec) {
      if (this.bucket.t !== 0) history = appendBucket(history, this.bucket)
      this.bucket = { t: sec, ingested: 0, delivered: 0, failed: 0, pending: 0, endpoints: {} }
    }
    this.bucket.ingested += f.ingested
    this.bucket.pending = f.totals.pending // a gauge: the last value in the second
    for (const e of f.endpoints) {
      this.bucket.delivered += e.ok
      this.bucket.failed += e.retry + e.dead
      this.bucket.endpoints[e.id] = (this.bucket.endpoints[e.id] ?? 0) + e.ok
    }

    // Newest first, bounded. Rows appear instantly (no animation) — see Feed.
    const feed = f.samples.length
      ? [...[...f.samples].sort((a, b) => b.at - a.at), ...this.snapshot.feed].slice(0, FEED_SIZE)
      : this.snapshot.feed

    this.publish({ frame: f, rates, history, feed })
  }

  pushNarration(n: Narration) {
    // A new scenario starts a fresh rail.
    const prev = this.snapshot.narration
    const sameScenario =
      prev.length > 0 &&
      prev[prev.length - 1].scenario === n.scenario &&
      !prev[prev.length - 1].done
    this.publish({ narration: sameScenario ? [...prev, n] : [n] })
  }

  addSuperseded(n: number) {
    this.publish({ superseded: this.snapshot.superseded + n })
  }
}

function computeRates(frames: Frame[]): Rates {
  if (frames.length === 0) return emptyRates
  const secs = (frames.length * (frames[0].window_ms || 100)) / 1000
  const r: Rates = { ingested: 0, delivered: 0, retried: 0, dead: 0, rateLimited: 0, endpoints: {} }
  for (const f of frames) {
    r.ingested += f.ingested
    for (const e of f.endpoints) {
      r.delivered += e.ok
      r.retried += e.retry
      r.dead += e.dead
      r.rateLimited += e.rate_limited
      const x = (r.endpoints[e.id] ??= {
        attempts: 0,
        ok: 0,
        retry: 0,
        dead: 0,
        rl: 0,
        p50: 0,
        p99: 0,
      })
      x.ok += e.ok
      x.retry += e.retry
      x.dead += e.dead
      x.rl += e.rate_limited
      if (e.p50_ms) x.p50 = e.p50_ms
      if (e.p99_ms) x.p99 = e.p99_ms
    }
  }
  r.ingested /= secs
  r.delivered /= secs
  r.retried /= secs
  r.dead /= secs
  r.rateLimited /= secs
  for (const x of Object.values(r.endpoints)) {
    x.ok /= secs
    x.retry /= secs
    x.dead /= secs
    x.rl /= secs
    x.attempts = x.ok + x.retry + x.dead + x.rl
  }
  return r
}

function appendBucket(h: Snapshot['history'], b: StreamStore['bucket']): Snapshot['history'] {
  const push = (arr: number[], v: number) => [...arr, v].slice(-HISTORY_SECONDS)
  const endpoints: Record<string, number[]> = {}
  const ids = new Set([...Object.keys(h.endpoints), ...Object.keys(b.endpoints)])
  for (const id of ids) endpoints[id] = push(h.endpoints[id] ?? [], b.endpoints[id] ?? 0)
  return {
    ingested: push(h.ingested, b.ingested),
    delivered: push(h.delivered, b.delivered),
    failed: push(h.failed, b.failed),
    pending: push(h.pending, b.pending),
    endpoints,
  }
}

export const stream = new StreamStore()

/**
 * Read a slice of stream state. The selector must return a primitive or a
 * reference that only changes when the underlying data does (every snapshot
 * field is replaced, never mutated, so selecting one is safe).
 */
export function useStream<T>(select: (s: Snapshot) => T): T {
  return useSyncExternalStore(stream.subscribe, () => select(stream.snapshot))
}
