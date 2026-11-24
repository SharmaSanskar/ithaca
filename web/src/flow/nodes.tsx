import { Handle, Position, type NodeProps } from '@xyflow/react'
import { RotateCcw } from 'lucide-react'
import type { ReactNode } from 'react'

import { Num } from '@/components/Num'
import { Sparkline } from '@/components/Sparkline'
import { useEndpointsById } from '@/lib/api'
import { useStream } from '@/lib/stream'
import { cn } from '@/lib/utils'

const NODE_W = 'w-[176px]'

/** Invisible connection points; the graph is not editable. */
function Handles() {
  const cls = '!h-1 !w-1 !min-w-0 !border-0 !bg-transparent'
  return (
    <>
      <Handle type="target" position={Position.Left} id="l" className={cls} isConnectable={false} />
      <Handle
        type="source"
        position={Position.Right}
        id="r"
        className={cls}
        isConnectable={false}
      />
      <Handle type="source" position={Position.Top} id="t" className={cls} isConnectable={false} />
      <Handle type="target" position={Position.Top} id="tt" className={cls} isConnectable={false} />
      <Handle
        type="source"
        position={Position.Bottom}
        id="b"
        className={cls}
        isConnectable={false}
      />
      <Handle
        type="target"
        position={Position.Bottom}
        id="bt"
        className={cls}
        isConnectable={false}
      />
      <Handle
        type="target"
        position={Position.Right}
        id="rt"
        className={cls}
        isConnectable={false}
      />
      <Handle
        type="source"
        position={Position.Left}
        id="ls"
        className={cls}
        isConnectable={false}
      />
    </>
  )
}

function Shell({
  kind,
  hero,
  sub,
  footer,
  className,
  badge,
}: {
  kind: string
  hero: ReactNode
  sub?: ReactNode
  footer?: ReactNode
  className?: string
  badge?: ReactNode
}) {
  return (
    <div className={cn('panel px-3 py-2.5', NODE_W, className)}>
      <Handles />
      <div className="flex items-center justify-between gap-2">
        <span className="t-micro font-mono text-fg-3">{kind}</span>
        {badge}
      </div>
      <div className="mt-1 t-metric font-medium text-fg">{hero}</div>
      {sub && <div className="mt-0.5 t-xs text-fg-3">{sub}</div>}
      {footer && <div className="mt-2">{footer}</div>}
    </div>
  )
}

function spark(points: number[], label: string, color?: string) {
  const hi = Math.max(1, ...points)
  return (
    <Sparkline
      points={points}
      domain={[0, hi]}
      width={150}
      height={16}
      label={label}
      color={color}
    />
  )
}

export function IngestNode(_: NodeProps) {
  const rate = useStream((s) => s.rates.ingested)
  const hist = useStream((s) => s.history.ingested)
  return (
    <Shell
      kind="Ingest"
      hero={<Num value={rate} format="rate" />}
      sub="events accepted · 202 after one transaction"
      footer={spark(hist, 'events ingested per second, last 60 seconds')}
    />
  )
}

export function QueueNode(_: NodeProps) {
  const pending = useStream((s) => s.frame?.totals.pending ?? null)
  const paused = useStream((s) => s.frame?.totals.paused_streams ?? 0)
  const hist = useStream((s) => s.history.pending)
  return (
    <Shell
      kind="Queue · Postgres"
      hero={<Num value={pending} />}
      sub={
        paused ? (
          <span className="text-st-dead-ink">
            {paused} ordered stream{paused > 1 ? 's' : ''} paused
          </span>
        ) : (
          'pending deliveries'
        )
      }
      footer={spark(hist, 'queue depth, last 60 seconds', 'var(--st-queued)')}
    />
  )
}

export function WorkersNode(_: NodeProps) {
  const pool = useStream((s) => s.frame?.pool ?? null)
  const workers = pool?.workers ?? 0
  const busy = pool?.busy ?? 0
  const cells = Math.min(workers, 64)
  return (
    <Shell
      kind="Worker pool"
      hero={
        <span>
          <Num value={pool ? busy : null} />
          <span className="t-sm text-fg-3"> / {workers || '—'}</span>
        </span>
      }
      sub={
        pool
          ? `peak busy per 100ms · ${pool.processes} dispatcher${pool.processes === 1 ? '' : 's'}`
          : 'no dispatcher reporting'
      }
      footer={
        workers > 64 ? (
          <div className="h-2.5 overflow-hidden rounded-[2px] bg-inset" aria-hidden>
            <div
              className="h-full bg-st-delivering"
              style={{ width: `${(busy / workers) * 100}%` }}
            />
          </div>
        ) : (
          // One cell per worker slot: the bounded pool, drawn literally.
          <div
            className="flex flex-wrap gap-[2px]"
            aria-label={`${busy} of ${workers} workers busy`}
            role="img"
          >
            {Array.from({ length: cells }, (_, i) => (
              <span
                key={i}
                className={cn(
                  'h-2.5 w-[4px] rounded-[1px]',
                  i < busy ? 'bg-st-delivering' : 'bg-line-strong',
                )}
              />
            ))}
          </div>
        )
      }
    />
  )
}

export function EndpointNode({ data }: NodeProps) {
  const id = (data as { id: string }).id
  const fe = useStream((s) => s.frame?.endpoints.find((e) => e.id === id))
  const r = useStream((s) => s.rates.endpoints[id])
  const hist = useStream((s) => s.history.endpoints[id])
  const ep = useEndpointsById().get(id)
  const circuit = fe?.circuit ?? ep?.circuit ?? 'closed'
  const attempts = r ? r.ok + r.retry + r.dead : 0
  const success = attempts > 0 && r ? r.ok / attempts : null
  const open = circuit !== 'closed'
  const disabled = fe && fe.state !== 'enabled'

  return (
    <div
      className={cn(
        'transition-[filter,opacity] duration-300',
        open && 'opacity-60 grayscale-[60%]',
      )}
    >
      <Shell
        kind="Endpoint"
        className={cn(open && 'border-st-failed')}
        badge={
          open ? (
            <span className="rounded-[3px] bg-st-failed/15 px-1 t-micro font-mono text-st-failed-ink">
              {circuit === 'half_open' ? 'probing' : 'circuit open'}
            </span>
          ) : disabled ? (
            <span className="t-micro font-mono text-fg-3">{fe?.state}</span>
          ) : null
        }
        hero={
          <span className="flex items-baseline justify-between gap-2">
            <span className="truncate t-md">{fe?.name ?? ep?.name ?? '…'}</span>
            <Num
              value={success}
              format="pct"
              className={cn('t-sm', success !== null && success < 0.9 && 'text-st-failed-ink')}
            />
          </span>
        }
        sub={
          <span className="flex justify-between gap-2">
            <span>
              p50 <Num value={r?.p50 || null} format="ms" /> · p99{' '}
              <Num value={r?.p99 || null} format="ms" />
            </span>
            <span>
              q <Num value={fe?.depth ?? null} />
            </span>
          </span>
        }
        footer={spark(
          hist ?? [],
          `${fe?.name ?? 'endpoint'} deliveries per second, last 60 seconds`,
          'var(--st-delivered)',
        )}
      />
      {open && ep?.circuit_next_probe_at && <ProbeCountdown at={ep.circuit_next_probe_at} />}
    </div>
  )
}

function ProbeCountdown({ at }: { at: string }) {
  // Re-render with each frame (10Hz) via the store, rather than a timer.
  useStream((s) => s.frame?.seq)
  const left = Math.max(0, new Date(at).getTime() - Date.now())
  return (
    <div className="mt-1 text-center t-micro font-mono text-fg-3">
      next probe in {(left / 1000).toFixed(1)}s
    </div>
  )
}

export function RetryNode(_: NodeProps) {
  const scheduled = useStream((s) => s.frame?.totals.retrying ?? null)
  const rate = useStream((s) => s.rates.retried + s.rates.rateLimited)
  return (
    <Shell
      kind="Retry · backoff"
      className="border-st-retrying/40"
      hero={<Num value={scheduled} />}
      sub={
        <span>
          waiting out a backoff · <Num value={rate} format="rate" />
        </span>
      }
    />
  )
}

export function DLQNode({ data }: NodeProps) {
  const { onReplay, replaying } = data as { onReplay: () => void; replaying: boolean }
  const dlq = useStream((s) => s.frame?.totals.dlq ?? null)
  const dots = Math.min(dlq ?? 0, 40)
  return (
    <Shell
      kind="Dead-letter queue"
      className="border-st-dead/40"
      hero={<Num value={dlq} className={dlq ? 'text-st-dead-ink' : undefined} />}
      sub="kept, not lost: replayable"
      footer={
        <div className="flex items-end justify-between gap-2">
          {/* Dead letters ACCRETE: they are still here, so the picture keeps them. */}
          <div className="flex w-[96px] flex-wrap gap-[3px]" aria-hidden>
            {Array.from({ length: dots }, (_, i) => (
              <span key={i} className="size-[5px] rounded-full bg-st-dead" />
            ))}
            {dlq !== null && dlq > 40 && (
              <span className="t-micro font-mono text-fg-3">+{dlq - 40}</span>
            )}
          </div>
          <button
            type="button"
            disabled={!dlq || replaying}
            onClick={onReplay}
            className="nodrag inline-flex shrink-0 items-center gap-1 rounded-[4px] border border-line-strong px-1.5 py-0.5 t-xs text-fg-2 hover:bg-hover disabled:opacity-40"
          >
            <RotateCcw size={11} /> {replaying ? 'Replaying' : 'Replay'}
          </button>
        </div>
      }
    />
  )
}
