/**
 * The WebSocket client.
 *
 * One same-origin socket, subscribed to `flow` (frames + deltas) and `demo`
 * (narration). Frames go to the StreamStore; deltas patch the TanStack Query
 * cache; narration goes to the store's rail.
 *
 * Reconnect uses FULL JITTER: delay = min(30s, 500ms·2^n) · (0.5 + rand).
 * A hub rollout disconnects every open dashboard at the same instant; without
 * jitter they would all reconnect in the same instant too — the thundering
 * herd this whole project is about.
 */
import type { QueryClient } from '@tanstack/react-query'

import { type Endpoint, type Overview, qk } from './api'
import { type Frame, type Narration, stream } from './stream'

type Delta =
  | { type: 'delta'; kind: 'endpoint'; id: string; rev: number; data: Endpoint }
  | { type: 'delta'; kind: 'dead_letter' | 'stream'; id: string; data: unknown }

export function startLive(qc: QueryClient): () => void {
  let ws: WebSocket | null = null
  let attempt = 0
  let stopped = false
  let timer: ReturnType<typeof setTimeout> | undefined

  const connect = () => {
    if (stopped) return
    stream.setConnection(attempt === 0 ? 'connecting' : 'reconnecting')
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
    const sock = new WebSocket(`${proto}//${location.host}/ws`)
    ws = sock

    // Retry from BOTH events, exactly once. Browsers fire error then close;
    // other runtimes fire only error on a refused connection.
    let scheduled = false
    const retry = () => {
      if (stopped || scheduled) return
      scheduled = true
      stream.setConnection('reconnecting')
      const delay = Math.min(30_000, 500 * 2 ** attempt) * (0.5 + Math.random())
      attempt++
      timer = setTimeout(connect, delay)
    }
    sock.onclose = retry
    sock.onerror = retry

    sock.onmessage = (e) => {
      const m = JSON.parse(e.data as string) as { type: string } & Record<string, unknown>
      switch (m.type) {
        case 'hello':
          attempt = 0
          sock.send(JSON.stringify({ op: 'subscribe', ch: 'flow' }))
          sock.send(JSON.stringify({ op: 'subscribe', ch: 'demo' }))
          // After a reconnect, anything may have changed while we were away:
          // refetch entities rather than trust a cache that missed deltas.
          void qc.invalidateQueries()
          break
        case 'subscribed':
          stream.setConnection('live')
          break
        case 'frame':
          stream.pushFrame(m as unknown as Frame)
          break
        case 'narration':
          stream.pushNarration(m as unknown as Narration)
          break
        case 'delta':
          applyDelta(qc, m as unknown as Delta)
          break
        case 'lag':
          stream.addSuperseded(Number(m.superseded) || 0)
          break
      }
    }
  }

  connect()
  return () => {
    stopped = true
    clearTimeout(timer)
    ws?.close()
  }
}

/**
 * Deltas PATCH cached entities, guarded by revision so an older delta can
 * never overwrite newer data. They never insert or reorder list rows: that
 * would corrupt keyset pagination. Lists that gain rows are invalidated.
 */
function applyDelta(qc: QueryClient, d: Delta) {
  if (d.kind === 'endpoint') {
    qc.setQueryData<Overview>(qk.overview, (old) => {
      if (!old) return old
      let found = false
      const endpoints = old.endpoints.map((e) => {
        if (e.id !== d.id) return e
        found = true
        return d.rev >= e.rev ? d.data : e
      })
      return found ? { ...old, endpoints } : old
    })
    return
  }
  if (d.kind === 'dead_letter' || d.kind === 'stream') {
    void qc.invalidateQueries({ queryKey: ['dlq'] })
  }
}
