import { useNavigate } from 'react-router'

import { useEndpointsById } from '@/lib/api'
import { STATUS_LABELS, type Status } from '@/lib/status'
import { useStream, type Sample } from '@/lib/stream'
import { ago } from '@/lib/time'

import { Num } from './Num'
import { StatusPill } from './StatusPill'
import { Empty } from './ui'

const toStatus: Record<Sample['outcome'], Status> = {
  ingested: 'queued',
  delivered: 'delivered',
  retry: 'failed',
  rate_limited: 'retrying',
  dead: 'dead',
}

/**
 * The live feed: sampled events, newest first.
 *
 * New rows appear INSTANTLY — no slide-in, no height animation, no stagger.
 * At 40 rows a second any of those induces nausea. Recency is a 600ms
 * left-border wash in the row's status color instead: color-only, zero layout.
 */
export function Feed({ limit = 40 }: { limit?: number }) {
  const feed = useStream((s) => s.feed)
  useStream((s) => Math.floor((s.frame?.t ?? 0) / 1000)) // re-render "ago" once a second
  const endpoints = useEndpointsById()
  const navigate = useNavigate()

  if (feed.length === 0) return <Empty>Waiting for events…</Empty>
  return (
    <ul className="divide-y divide-line-faint" aria-label="Live event feed">
      {feed.slice(0, limit).map((s) => {
        const st = toStatus[s.outcome]
        return (
          <li key={`${s.delivery_id ?? s.event_id}-${s.outcome}-${s.at}`}>
            <button
              type="button"
              onClick={() => navigate(`/events/${s.event_id}`)}
              className="recency flex h-[var(--row-h)] w-full items-center gap-2 px-3 text-left hover:bg-hover"
              style={{ '--wash': `var(--st-${st})` } as React.CSSProperties}
            >
              <StatusPill status={st} />
              <span className="min-w-0 flex-1 truncate t-sm text-fg">{s.type}</span>
              <span className="hidden w-24 truncate t-xs text-fg-3 xl:inline">
                {s.endpoint_id ? (endpoints.get(s.endpoint_id)?.name ?? '') : 'ingest'}
              </span>
              <span className="w-12 text-right t-xs text-fg-3">
                {s.latency_ms ? (
                  <Num value={s.latency_ms} format="ms" />
                ) : s.attempt && s.attempt > 1 ? (
                  `try ${s.attempt}`
                ) : (
                  ''
                )}
              </span>
              <span
                className="w-12 text-right t-micro font-mono text-fg-3"
                aria-label={STATUS_LABELS[st]}
              >
                {ago(s.at)}
              </span>
            </button>
          </li>
        )
      })}
    </ul>
  )
}
