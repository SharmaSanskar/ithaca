import { ArrowLeft } from 'lucide-react'
import { useState } from 'react'
import { Link, useParams } from 'react-router'

import { AttemptTimeline } from '@/components/AttemptTimeline'
import { IdChip } from '@/components/IdChip'
import { Num } from '@/components/Num'
import { StatusPill } from '@/components/StatusPill'
import { Empty, Panel, Skeleton, Stat } from '@/components/ui'
import { useEndpointsById, useEvent, type Delivery } from '@/lib/api'
import type { Status } from '@/lib/status'
import { clock } from '@/lib/time'

const deliveryStatus: Record<Delivery['status'], Status> = {
  pending: 'queued',
  retrying: 'retrying',
  succeeded: 'delivered',
  dead: 'dead',
}

/** One event: the exact payload, and every delivery's retry ladder. */
export default function EventDetail() {
  const { id = '' } = useParams()
  const q = useEvent(id)
  const endpoints = useEndpointsById()

  if (q.isPending) return <Skeleton className="h-64" />
  if (q.isError || !q.data)
    return <Empty>Event not found. It may have aged out of the demo's retention window.</Empty>
  const ev = q.data

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-3">
        <Link to="/events" className="text-fg-3 hover:text-fg" aria-label="Back to events">
          <ArrowLeft size={16} />
        </Link>
        <h1 className="t-lg font-semibold text-fg">{ev.type}</h1>
        <IdChip id={ev.id} head={12} tail={6} />
      </div>
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(0,1fr)_420px]">
        <div className="flex flex-col gap-3">
          {ev.deliveries.length === 0 && (
            <Panel title="Deliveries">
              <Empty>No subscription matched this event type.</Empty>
            </Panel>
          )}
          {ev.deliveries.map((d) => (
            <DeliveryCard
              key={d.id}
              d={d}
              createdAt={ev.created_at}
              endpoint={endpoints.get(d.endpoint_id)?.name ?? 'endpoint'}
            />
          ))}
        </div>
        <div className="flex flex-col gap-3">
          <Panel title="Event" bodyClassName="grid grid-cols-2 gap-3 p-3">
            <Stat label="Received">{clock(ev.created_at)}</Stat>
            <Stat label="Ordering key">
              <span className="font-mono t-xs">{ev.ordering_key ?? '—'}</span>
            </Stat>
            <Stat label="Deliveries">
              <Num value={ev.deliveries.length} />
            </Stat>
            <Stat label="Ithaca-Id header">
              <IdChip id={ev.id} />
            </Stat>
          </Panel>
          <Panel title="Payload · exact bytes as received" bodyClassName="p-3">
            <pre className="max-h-[420px] overflow-auto rounded-[4px] bg-inset p-3 t-xs text-fg-2">
              {JSON.stringify(ev.payload, null, 2)}
            </pre>
            <p className="mt-2 t-xs text-fg-3">
              Stored as raw bytes and signed as-is: re-serializing JSON would reorder keys and break
              every subscriber&rsquo;s HMAC check.
            </p>
          </Panel>
        </div>
      </div>
    </div>
  )
}

function DeliveryCard({
  d,
  createdAt,
  endpoint,
}: {
  d: Delivery
  createdAt: string
  endpoint: string
}) {
  const [sel, setSel] = useState<number | null>(d.history.length ? d.history.length - 1 : null)
  const a = sel !== null ? d.history[sel] : undefined
  const open = d.status === 'pending' || d.status === 'retrying'

  return (
    <Panel
      title={
        <span className="flex items-center gap-2 normal-case tracking-normal">
          <span className="t-sm text-fg">{endpoint}</span>
          <span className="text-fg-3">seq {d.seq}</span>
          {d.replay_of && <span className="text-st-delivered-ink">replay</span>}
        </span>
      }
      actions={
        <span className="flex items-center gap-2">
          {d.attempts > 0 && (
            <span className="t-xs text-fg-3">
              {d.attempts} attempt{d.attempts > 1 ? 's' : ''}
            </span>
          )}
          <StatusPill status={deliveryStatus[d.status]} />
        </span>
      }
      bodyClassName="flex flex-col gap-3 p-3"
    >
      {d.history.length === 0 && !open ? (
        <p className="t-sm text-fg-3">No attempts recorded.</p>
      ) : (
        <AttemptTimeline
          createdAt={createdAt}
          attempts={d.history}
          nextAttemptAt={open ? d.next_attempt_at : undefined}
          dead={d.status === 'dead'}
          selected={sel}
          onSelect={setSel}
        />
      )}
      {a && (
        <div
          role="tabpanel"
          className="grid grid-cols-2 gap-3 rounded-[4px] bg-inset p-3 md:grid-cols-4"
        >
          <Stat label={`Attempt ${a.attempt}`}>{a.outcome.replace('_', ' ')}</Stat>
          <Stat label="Response">
            {a.status_code ? `HTTP ${a.status_code}` : (a.error_class ?? '—')}
          </Stat>
          <Stat label="Duration">
            <Num value={a.duration_ms} format="ms" />
          </Stat>
          <Stat label="Started">{clock(a.started_at)}</Stat>
          {a.error && (
            <p className="col-span-full font-mono t-xs break-all text-st-failed-ink">{a.error}</p>
          )}
        </div>
      )}
      {d.dead_reason && (
        <p className="t-xs text-st-dead-ink">
          Dead-lettered: {d.dead_reason}.{' '}
          <Link to="/dlq" className="underline">
            Open the DLQ
          </Link>{' '}
          to replay it.
        </p>
      )}
    </Panel>
  )
}
