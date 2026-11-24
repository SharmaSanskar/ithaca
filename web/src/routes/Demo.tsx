import { Play, RotateCcw } from 'lucide-react'
import { useEffect, useState } from 'react'

import { NarrationRail } from '@/components/Demo'
import { Num } from '@/components/Num'
import { Button, Panel } from '@/components/ui'
import { FlowCanvas } from '@/flow/FlowCanvas'
import { useDemoAction, useDemoState, type SinkBehavior } from '@/lib/api'
import { cn } from '@/lib/utils'

const SINKS = [
  { id: 'orders', name: 'orders-service', note: 'order.* · ordered per order id' },
  { id: 'billing', name: 'billing', note: 'invoice.* · payment.*' },
  { id: 'crm', name: 'crm', note: 'user.*' },
  { id: 'analytics', name: 'analytics', note: 'every event' },
]

/**
 * The demo console. Everything here is a request to the SERVER: scenarios
 * run in the worker service against the real pipeline, sinks are real HTTP
 * endpoints in receiver-sim. This page changes nothing locally and simulates
 * nothing; it watches the same live feed as everyone else.
 */
export default function Demo() {
  const { data } = useDemoState()
  const act = useDemoAction()
  const error = act.error instanceof Error ? act.error.message : null

  return (
    <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(0,1fr)_400px]">
      <div className="flex min-w-0 flex-col gap-3">
        <Panel title="Live pipeline" bodyClassName="overflow-hidden">
          <FlowCanvas height={430} />
        </Panel>
        <Panel
          title={data?.scenario ? `Narration · ${data.scenario.title}` : 'Narration'}
          bodyClassName="p-3"
        >
          <NarrationRail max={12} />
        </Panel>
      </div>

      <div className="flex flex-col gap-3">
        <Panel title="Scenarios" bodyClassName="divide-y divide-line-faint">
          {(data?.scenarios ?? []).map((s) => {
            const active = data?.scenario?.name === s.name
            return (
              <div key={s.name} className="flex items-start gap-3 px-3 py-2.5">
                <div className="min-w-0 flex-1">
                  <div className="t-sm font-medium text-fg">{s.title}</div>
                  <p className="mt-0.5 t-xs text-fg-3">{s.description}</p>
                </div>
                <Button
                  size="xs"
                  variant={active ? 'primary' : 'secondary'}
                  disabled={act.isPending}
                  onClick={() => act.mutate({ path: `/scenarios/${s.name}` })}
                  aria-label={`Run scenario: ${s.title}`}
                >
                  <Play size={10} />
                  {active ? `${data?.scenario?.step}/${data?.scenario?.total}` : 'Run'}
                </Button>
              </div>
            )
          })}
          {!data?.leader && (
            <p className="px-3 py-2 t-xs text-st-retrying-ink">
              The demo runtime is offline (is the worker service running?).
            </p>
          )}
        </Panel>

        <Panel title="Traffic" bodyClassName="p-3">
          <Traffic
            current={data?.traffic_rate ?? 0}
            onCommit={(rate) => act.mutate({ path: '/traffic', body: { rate } })}
          />
        </Panel>

        <Panel title="Endpoints · break them" bodyClassName="divide-y divide-line-faint">
          {SINKS.map((s) => (
            <SinkControl
              key={s.id}
              sink={s}
              behavior={data?.sinks[s.id]}
              onCommit={(b) => act.mutate({ path: `/sinks/${s.id}`, method: 'PUT', body: b })}
            />
          ))}
        </Panel>
        {error && <p className="t-xs text-st-failed-ink">{error}</p>}
        <p className="t-xs text-fg-3">
          Controls are rate-limited per visitor. Changes apply on release, not on every pixel of a
          slider.
        </p>
      </div>
    </div>
  )
}

function Traffic({ current, onCommit }: { current: number; onCommit: (r: number) => void }) {
  const [v, setV] = useState(current)
  useEffect(() => setV(current), [current])
  return (
    <label className="flex flex-col gap-2">
      <span className="flex justify-between t-sm text-fg-2">
        Events per second <Num value={v} format="rate" className="text-fg" />
      </span>
      <input
        type="range"
        min={0}
        max={200}
        step={5}
        value={v}
        onChange={(e) => setV(+e.target.value)}
        onPointerUp={() => onCommit(v)}
        onKeyUp={() => onCommit(v)}
        className="accent-[var(--signal)]"
      />
    </label>
  )
}

function SinkControl({
  sink,
  behavior,
  onCommit,
}: {
  sink: { id: string; name: string; note: string }
  behavior?: SinkBehavior
  onCommit: (b: Partial<SinkBehavior>) => void
}) {
  const [b, setB] = useState<SinkBehavior>({
    status: 200,
    latency_ms: 40,
    fail_rate: 0,
    fail_status: 503,
  })
  useEffect(() => {
    if (behavior) setB({ ...behavior, fail_status: behavior.fail_status || 503 })
  }, [behavior])
  const broken = b.fail_rate > 0 || b.latency_ms > 500
  const commit = (next: SinkBehavior) =>
    onCommit({
      status: 200,
      latency_ms: next.latency_ms,
      fail_rate: next.fail_rate,
      fail_status: next.fail_status,
    })

  return (
    <div className="flex flex-col gap-2 px-3 py-2.5">
      <div className="flex items-center justify-between">
        <div>
          <span className={cn('t-sm font-medium', broken ? 'text-st-retrying-ink' : 'text-fg')}>
            {sink.name}
          </span>
          <span className="ml-2 t-xs text-fg-3">{sink.note}</span>
        </div>
        <Button
          size="xs"
          variant="ghost"
          aria-label={`Reset ${sink.name} to healthy`}
          onClick={() => {
            const healthy = { status: 200, latency_ms: 40, fail_rate: 0, fail_status: 503 }
            setB(healthy)
            commit(healthy)
          }}
        >
          <RotateCcw size={11} /> healthy
        </Button>
      </div>
      <Slider
        label="Latency"
        value={b.latency_ms}
        display={<Num value={b.latency_ms} format="ms" />}
        min={0}
        max={3000}
        step={20}
        onChange={(latency_ms) => setB({ ...b, latency_ms })}
        onCommit={() => commit(b)}
      />
      <Slider
        label="Failure rate"
        value={Math.round(b.fail_rate * 100)}
        display={<Num value={b.fail_rate} format="pct" />}
        min={0}
        max={100}
        step={5}
        onChange={(p) => setB({ ...b, fail_rate: p / 100 })}
        onCommit={() => commit(b)}
      />
      <label className="flex items-center justify-between t-xs text-fg-3">
        Fails with
        <select
          value={b.fail_status}
          onChange={(e) => {
            const next = { ...b, fail_status: +e.target.value }
            setB(next)
            commit(next)
          }}
          className="rounded-[4px] border border-line-strong bg-surface px-1.5 py-0.5 t-xs font-mono text-fg"
        >
          <option value={503}>503 Unavailable · retried</option>
          <option value={500}>500 Server error · retried</option>
          <option value={429}>429 Too many · deferred, no attempt used</option>
          <option value={400}>400 Bad request · dead-lettered</option>
          <option value={410}>410 Gone · endpoint disabled</option>
        </select>
      </label>
    </div>
  )
}

function Slider(props: {
  label: string
  value: number
  display: React.ReactNode
  min: number
  max: number
  step: number
  onChange: (v: number) => void
  onCommit: () => void
}) {
  return (
    <label className="grid grid-cols-[80px_1fr_56px] items-center gap-2 t-xs text-fg-3">
      {props.label}
      <input
        type="range"
        min={props.min}
        max={props.max}
        step={props.step}
        value={props.value}
        onChange={(e) => props.onChange(+e.target.value)}
        onPointerUp={props.onCommit}
        onKeyUp={props.onCommit}
        className="accent-[var(--signal)]"
      />
      <span className="text-right text-fg">{props.display}</span>
    </label>
  )
}
