import { Num } from './Num'
import { Sparkline } from './Sparkline'
import { useStream } from '@/lib/stream'
import { cn } from '@/lib/utils'

function Tile({
  label,
  children,
  sub,
  spark,
  tone,
}: {
  label: string
  children: React.ReactNode
  sub?: React.ReactNode
  spark?: number[]
  tone?: string
}) {
  return (
    <div className="panel flex flex-col justify-between gap-1 px-3 py-2.5">
      <span className="t-micro font-mono text-fg-3">{label}</span>
      <div className="flex items-end justify-between gap-2">
        <span className={cn('t-hero font-medium text-fg', tone)}>{children}</span>
        {spark && spark.length > 1 && (
          <Sparkline
            points={spark}
            domain={[0, Math.max(1, ...spark)]}
            width={80}
            height={22}
            label={`${label}, last 60 seconds`}
          />
        )}
      </div>
      <span className="t-xs text-fg-3">{sub}</span>
    </div>
  )
}

/** The five numbers that summarize the system's health right now. */
export function StatTiles() {
  const r = useStream((s) => s.rates)
  const f = useStream((s) => s.frame)
  const h = useStream((s) => s.history)
  const open = f?.endpoints.filter((e) => e.circuit !== 'closed').length ?? 0
  const attempts = r.delivered + r.retried + r.dead
  const p99 = Math.max(0, ...Object.values(r.endpoints).map((e) => e.p99))

  return (
    <div className="grid grid-cols-2 gap-2 md:grid-cols-3 xl:grid-cols-5">
      <Tile label="Ingest" spark={h.ingested} sub="events accepted per second">
        <Num value={f ? r.ingested : null} format="rate" flash />
      </Tile>
      <Tile
        label="Delivered"
        spark={h.delivered}
        sub={
          <>
            success rate <Num value={attempts ? r.delivered / attempts : null} format="pct" />
          </>
        }
      >
        <Num value={f ? r.delivered : null} format="rate" />
      </Tile>
      <Tile
        label="Queue depth"
        spark={h.pending}
        sub={
          <>
            p99 delivery <Num value={p99 || null} format="ms" />
          </>
        }
      >
        <Num value={f?.totals.pending ?? null} flash />
      </Tile>
      <Tile
        label="Circuits open"
        tone={open ? 'text-st-failed-ink' : undefined}
        sub={`of ${f?.endpoints.length ?? 0} endpoints`}
      >
        <Num value={f ? open : null} />
      </Tile>
      <Tile
        label="Dead letters"
        tone={f?.totals.dlq ? 'text-st-dead-ink' : undefined}
        sub="kept, replayable"
      >
        <Num value={f?.totals.dlq ?? null} flash />
      </Tile>
    </div>
  )
}
