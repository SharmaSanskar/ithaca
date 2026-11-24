import { useMemo } from 'react'
import type { AlignedData } from 'uplot'

import { NarrationRail, ScenarioBar } from '@/components/Demo'
import { Feed } from '@/components/Feed'
import { IntroOverlay } from '@/components/IntroOverlay'
import { StatTiles } from '@/components/StatTiles'
import { TimeSeries, type Series } from '@/components/TimeSeries'
import { Panel } from '@/components/ui'
import { FlowCanvas } from '@/flow/FlowCanvas'
import { useDemoAction, useSeries } from '@/lib/api'

const series: Series[] = [
  { label: 'ingested/s', color: '--q-1' },
  { label: 'delivered/s', color: '--st-delivered', fill: true },
  { label: 'failed/s', color: '--st-failed' },
  { label: 'p99 ms', color: '--st-retrying', right: true, dash: [4, 3] },
]

export default function MissionControl() {
  const act = useDemoAction()
  return (
    <div className="flex flex-col gap-3">
      <IntroOverlay onStart={() => act.mutate({ path: '/scenarios/endpoint_down' })} />
      <StatTiles />

      <div className="panel flex flex-col gap-3 px-3 py-2.5">
        <ScenarioBar />
        <NarrationRail max={3} />
      </div>

      <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(0,1fr)_380px]">
        <Panel title="Live pipeline" bodyClassName="overflow-hidden">
          <FlowCanvas height={500} />
        </Panel>
        <Panel title="Live events · sampled" bodyClassName="max-h-[500px] overflow-y-auto">
          <Feed />
        </Panel>
      </div>

      <Panel title="Throughput & latency · last 15 minutes" bodyClassName="px-2 pt-2">
        <Throughput />
      </Panel>
    </div>
  )
}

function Throughput() {
  const { data } = useSeries(900)
  const aligned = useMemo<AlignedData>(() => {
    const pts = data?.data ?? []
    const per = data?.bucket_seconds ?? 10
    return [
      pts.map((p) => p.t),
      pts.map((p) => p.ingested / per),
      pts.map((p) => p.succeeded / per),
      pts.map((p) => p.failed / per),
      pts.map((p) => p.p99_ms),
    ]
  }, [data])
  return (
    <TimeSeries
      data={aligned}
      series={series}
      height={170}
      leftUnit="/s"
      rightUnit="ms"
      label="Ingest and delivery throughput with p99 latency over the last 15 minutes"
    />
  )
}
