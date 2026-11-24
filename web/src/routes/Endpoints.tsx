import { useQueries } from '@tanstack/react-query'

import { Num } from '@/components/Num'
import { Sparkline } from '@/components/Sparkline'
import { Table, Td, Th } from '@/components/Table'
import { Empty, Panel } from '@/components/ui'
import { qk, useOverview, type Endpoint, type SeriesPoint } from '@/lib/api'
import { useStream } from '@/lib/stream'
import { cn } from '@/lib/utils'

/**
 * Endpoints, compared with small multiples: one sparkline per row on a SHARED
 * y-domain, so a 4/min endpoint and a 4,000/min one look as different as they
 * are. (No categorical palette anywhere: comparison is by position, not hue.)
 */
export default function Endpoints() {
  const { data } = useOverview()
  const endpoints = data?.endpoints ?? []
  const series = useQueries({
    queries: endpoints.map((e) => ({
      queryKey: qk.series(900, e.id),
      queryFn: async () => {
        const r = await fetch(`/api/v1/dash/series?window=900&bucket=30&endpoint_id=${e.id}`)
        return ((await r.json()) as { data: SeriesPoint[] }).data
      },
      refetchInterval: 15_000,
    })),
  })
  const max = Math.max(
    1,
    ...series.flatMap((s) => (s.data ?? []).map((p) => p.succeeded + p.failed)),
  )

  if (endpoints.length === 0) return <Empty>No endpoints.</Empty>
  return (
    <Panel title="Endpoints">
      <Table
        head={
          <>
            <Th>Endpoint</Th>
            <Th>State</Th>
            <Th>Circuit</Th>
            <Th className="text-right">Queue</Th>
            <Th className="text-right">Success · 1s</Th>
            <Th className="text-right">p50 / p99</Th>
            <Th>Deliveries · 15 min (shared scale)</Th>
          </>
        }
      >
        {endpoints.map((e, i) => (
          <Row key={e.id} e={e} points={series[i]?.data ?? []} max={max} />
        ))}
      </Table>
    </Panel>
  )
}

function Row({ e, points, max }: { e: Endpoint; points: SeriesPoint[]; max: number }) {
  const live = useStream((s) => s.rates.endpoints[e.id])
  const fe = useStream((s) => s.frame?.endpoints.find((x) => x.id === e.id))
  const circuit = fe?.circuit ?? e.circuit
  const attempts = live ? live.ok + live.retry + live.dead : 0
  return (
    <tr className="hover:bg-hover">
      <Td className="py-1.5">
        <div className="t-sm text-fg">{e.name}</div>
        <div className="max-w-[280px] truncate font-mono t-xs text-fg-3">{e.url}</div>
      </Td>
      <Td className={cn('t-xs', e.state === 'enabled' ? 'text-fg-2' : 'text-st-retrying-ink')}>
        {e.state.replace('_', ' ')}
      </Td>
      <Td>
        <span
          className={cn(
            'rounded-[3px] px-1.5 py-0.5 t-micro font-mono',
            circuit === 'closed'
              ? 'bg-st-delivered/12 text-st-delivered-ink'
              : 'bg-st-failed/15 text-st-failed-ink',
          )}
        >
          {circuit.replace('_', '-')}
        </span>
        {e.consecutive_failures > 0 && (
          <span className="ml-2 t-xs text-fg-3">{e.consecutive_failures} in a row</span>
        )}
      </Td>
      <Td className="text-right">
        <Num value={fe?.depth ?? e.depth} />
      </Td>
      <Td className="text-right">
        <Num value={attempts ? (live?.ok ?? 0) / attempts : null} format="pct" />
      </Td>
      <Td className="text-right t-xs">
        <Num value={live?.p50 || null} format="ms" /> /{' '}
        <Num value={live?.p99 || null} format="ms" />
      </Td>
      <Td>
        <Sparkline
          points={points.map((p) => p.succeeded)}
          failures={points.map((p) => p.failed)}
          domain={[0, max]}
          width={160}
          height={22}
          label={`${e.name} deliveries over 15 minutes`}
        />
      </Td>
    </tr>
  )
}
