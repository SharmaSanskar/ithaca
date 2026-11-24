import '@xyflow/react/dist/base.css'

import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type EdgeProps,
  type Node,
} from '@xyflow/react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { Num } from '@/components/Num'
import { useReplay } from '@/lib/api'
import { prefersReducedMotion } from '@/lib/motion'
import { stream, useStream, type Snapshot } from '@/lib/stream'
import { useTheme } from '@/lib/theme-context'

import { DLQNode, EndpointNode, IngestNode, QueueNode, RetryNode, WorkersNode } from './nodes'
import { emissionsFor, ParticleEngine, sampleEdge } from './particles'

const nodeTypes = {
  ingest: IngestNode,
  queue: QueueNode,
  workers: WorkersNode,
  endpoint: EndpointNode,
  retry: RetryNode,
  dlq: DLQNode,
}
const edgeTypes = { rate: RateEdge }
const MAX_ENDPOINTS = 6

/**
 * The live flow canvas. React Flow renders the graph chrome (nodes, edge
 * paths, pan/zoom); it never renders a single particle. Particles live on one
 * <canvas> overlay, driven by ParticleEngine outside React.
 *
 * Geometry tells the story. The happy path is a straight horizontal spine:
 * ingest → queue → workers → endpoints. Retries arc ABOVE it and come back.
 * Dead letters go BELOW it, on the only edge in the graph with no return path.
 */
export function FlowCanvas({ height = 460 }: { height?: number }) {
  return (
    <ReactFlowProvider>
      <Graph height={height} />
    </ReactFlowProvider>
  )
}

function Graph({ height }: { height: number }) {
  const rf = useReactFlow()
  const wrap = useRef<HTMLDivElement>(null)
  const canvas = useRef<HTMLCanvasElement>(null)
  const engine = useRef<ParticleEngine | null>(null)
  const { resolvedTheme } = useTheme()
  const replay = useReplay()
  const [replayingUntil, setReplayingUntil] = useState(0)
  const replaying = replayingUntil > Date.now()

  // Endpoints come from the frames (real, ordered by creation), capped.
  const endpointIds = useStream((s) => endpointKey(s))
  const ids = useMemo(() => (endpointIds ? endpointIds.split(',') : []), [endpointIds])

  const onReplay = useCallback(() => {
    replay.mutate({ all: true })
    setReplayingUntil(Date.now() + 8000)
    setTimeout(() => setReplayingUntil(0), 8000)
  }, [replay])

  const { nodes, edges } = useMemo(
    () => layout(ids, onReplay, replaying),
    [ids, onReplay, replaying],
  )

  // The particle engine: one per mount, fed by the store, drawn by rAF.
  useEffect(() => {
    if (!canvas.current) return
    const e = new ParticleEngine(canvas.current, () => rf.getViewport(), prefersReducedMotion)
    engine.current = e
    e.start()
    const off = stream.onFrame((f) => e.emit(emissionsFor(f), f.window_ms || 100))
    return () => {
      off()
      e.stop()
      engine.current = null
    }
  }, [rf])

  useEffect(() => {
    engine.current?.refreshColors()
  }, [resolvedTheme])

  // Edge geometry is sampled from the rendered SVG paths into lookup tables.
  // Re-sampled each second: cheap (a few dozen getPointAtLength calls), and it
  // follows layout changes and resizes without bookkeeping.
  useEffect(() => {
    const sample = () => {
      const geoms = []
      for (const e of edges) {
        const el = wrap.current?.querySelector<SVGPathElement>(
          `[data-testid="rf__edge-${e.id}"] path.react-flow__edge-path`,
        )
        if (el && el.getTotalLength() > 0) geoms.push(sampleEdge(e.id, el))
      }
      engine.current?.setEdges(geoms)
    }
    const first = setTimeout(sample, 50)
    const id = setInterval(sample, 1000)
    return () => {
      clearTimeout(first)
      clearInterval(id)
    }
  }, [edges])

  // Keep the whole pipeline in view as the panel resizes.
  useEffect(() => {
    if (!wrap.current) return
    const ro = new ResizeObserver(() => void rf.fitView({ padding: 0.08, duration: 0 }))
    ro.observe(wrap.current)
    return () => ro.disconnect()
  }, [rf, ids.length])

  return (
    <div ref={wrap} className="relative w-full" style={{ height }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.08 }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        zoomOnScroll={false}
        zoomOnDoubleClick={false}
        panOnDrag={false}
        preventScrolling={false}
        minZoom={0.3}
      />
      <canvas
        ref={canvas}
        className="pointer-events-none absolute inset-0 h-full w-full"
        aria-hidden
      />
      <CanvasStatus engine={engine} />
      <A11ySummary />
    </div>
  )
}

function endpointKey(s: Snapshot) {
  return (s.frame?.endpoints ?? [])
    .slice(0, MAX_ENDPOINTS)
    .map((e) => e.id)
    .join(',')
}

// ── layout: hand-authored, because the topology never changes ───────────────

function layout(
  ids: string[],
  onReplay: () => void,
  replaying: boolean,
): { nodes: Node[]; edges: Edge[] } {
  const SP = 112 // vertical spacing between endpoint nodes
  // The spine sits on y≈0 with generous gaps, so every edge has room for its
  // rate label. Retry floats above the queue→workers gap; DLQ sits below and
  // right of the workers, "beneath the floor" of the happy path.
  const nodes: Node[] = [
    { id: 'ingest', type: 'ingest', position: { x: 0, y: -52 }, data: {} },
    { id: 'queue', type: 'queue', position: { x: 290, y: -52 }, data: {} },
    { id: 'workers', type: 'workers', position: { x: 580, y: -58 }, data: {} },
    { id: 'retry', type: 'retry', position: { x: 435, y: -240 }, data: {} },
    { id: 'dlq', type: 'dlq', position: { x: 700, y: 180 }, data: { onReplay, replaying } },
  ]
  const top = -((ids.length - 1) * SP) / 2 - 52
  ids.forEach((id, i) =>
    nodes.push({
      id: 'ep-' + id,
      type: 'endpoint',
      position: { x: 960, y: top + i * SP },
      data: { id },
    }),
  )

  const e = (
    id: string,
    source: string,
    target: string,
    sh: string,
    th: string,
    data: RateEdgeData,
  ): Edge => ({
    id,
    source,
    target,
    sourceHandle: sh,
    targetHandle: th,
    type: 'rate',
    data: data as unknown as Record<string, unknown>,
  })
  const edges: Edge[] = [
    e('ingest-queue', 'ingest', 'queue', 'r', 'l', {
      metric: 'ingested',
      color: 'var(--line-strong)',
    }),
    e('queue-workers', 'queue', 'workers', 'r', 'l', {
      metric: 'attempts',
      color: 'var(--line-strong)',
    }),
    // Retries go UP and come back.
    e('workers-retry', 'workers', 'retry', 't', 'rt', {
      metric: 'retried',
      color: 'var(--st-retrying)',
      dashed: true,
    }),
    e('retry-queue', 'retry', 'queue', 'ls', 'tt', {
      metric: 'scheduled',
      color: 'var(--st-retrying)',
      dashed: true,
    }),
    // Dead letters go DOWN and stay: no edge leaves the DLQ (except a replay).
    e('workers-dlq', 'workers', 'dlq', 'b', 'l', { metric: 'dead', color: 'var(--st-dead)' }),
    ...ids.map((id) =>
      e('workers-' + id, 'workers', 'ep-' + id, 'r', 'l', {
        metric: 'endpoint',
        endpoint: id,
        color: 'var(--line-strong)',
      }),
    ),
  ]
  if (replaying) {
    // The payoff shot: a temporary green edge back up into the queue.
    edges.push(
      e('dlq-queue', 'dlq', 'queue', 'b', 'bt', {
        metric: 'replay',
        color: 'var(--st-delivered)',
        dashed: true,
        animated: true,
      }),
    )
  }
  return { nodes, edges }
}

// ── edges: every edge carries its REAL rate as a permanent label ────────────

interface RateEdgeData {
  metric: 'ingested' | 'attempts' | 'retried' | 'scheduled' | 'dead' | 'endpoint' | 'replay'
  endpoint?: string
  color: string
  dashed?: boolean
  animated?: boolean
}

function edgeRate(s: Snapshot, d: RateEdgeData): number | null {
  const r = s.rates
  switch (d.metric) {
    case 'ingested':
      return r.ingested
    case 'attempts':
      return r.delivered + r.retried + r.dead + r.rateLimited
    case 'retried':
      return r.retried + r.rateLimited
    case 'dead':
      return r.dead
    case 'endpoint':
      return d.endpoint ? (r.endpoints[d.endpoint]?.attempts ?? 0) : 0
    default:
      return null
  }
}

function RateEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}: EdgeProps) {
  const d = data as unknown as RateEdgeData
  const [path, lx, ly] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    curvature: 0.35,
  })
  const rate = useStream((s) => edgeRate(s, d))
  // An open circuit CUTS the edge into its endpoint: a dashed red line, with no
  // particles on it (its streams are parked, so nothing is being sent).
  const cut = useStream((s) =>
    d.metric === 'endpoint' && d.endpoint
      ? (s.frame?.endpoints.find((e) => e.id === d.endpoint)?.circuit ?? 'closed') !== 'closed'
      : false,
  )
  const scheduled = useStream((s) =>
    d.metric === 'scheduled' ? (s.frame?.totals.retrying ?? 0) : 0,
  )
  // Particles are capped at 12/s per edge: above that each one stands for
  // several events, and the label admits it.
  const ratio = rate !== null && rate > 12 ? Math.round(rate / 12) : 1

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        style={{
          stroke: cut ? 'var(--st-failed)' : d.color,
          strokeWidth: cut ? 2 : 1.25,
          strokeDasharray: cut ? '3 5' : d.dashed ? '4 4' : undefined,
          animation: d.animated ? 'ithaca-dash 0.6s linear infinite' : undefined,
        }}
      />
      <EdgeLabelRenderer>
        <div
          className="nodrag nopan pointer-events-none absolute flex items-center gap-1 rounded-[3px] bg-canvas/85 px-1 t-micro font-mono text-fg-3"
          // Lifted just above the line, so particles pass under the label, not through it.
          style={{ transform: `translate(-50%, -100%) translate(${lx}px, ${ly - 3}px)` }}
        >
          {d.metric === 'scheduled' ? (
            <span>
              <Num value={scheduled} /> scheduled
            </span>
          ) : d.metric === 'replay' ? (
            <span className="text-st-delivered-ink">replaying →</span>
          ) : (
            <>
              <Num value={rate} format="rate" />
              {ratio > 1 && <span className="text-fg-3">~1:{ratio}</span>}
            </>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  )
}

// ── honesty and accessibility overlays ──────────────────────────────────────

function CanvasStatus({ engine }: { engine: React.RefObject<ParticleEngine | null> }) {
  const connection = useStream((s) => s.connection)
  const [sampled, setSampled] = useState(false)
  useEffect(() => {
    const id = setInterval(() => setSampled(engine.current?.sampled ?? false), 500)
    return () => clearInterval(id)
  }, [engine])
  return (
    <div className="pointer-events-none absolute top-2 right-2 flex gap-2 t-micro font-mono text-fg-3">
      {sampled && <span className="rounded-[3px] border border-line px-1">sampled</span>}
      {connection !== 'live' && (
        <span className="rounded-[3px] border border-line px-1 text-st-retrying-ink">
          {connection}
        </span>
      )}
    </div>
  )
}

/**
 * Screen readers get a summary regenerated at most once a second, and an
 * aria-live region that announces only THRESHOLD CROSSINGS (a circuit opened,
 * the DLQ crossed a multiple of 50) — never individual events, which would
 * make the page unusable with a screen reader.
 */
function A11ySummary() {
  const [summary, setSummary] = useState('Waiting for live data.')
  const [announce, setAnnounce] = useState('')
  const prev = useRef<{ open: Set<string>; dlq: number }>({ open: new Set(), dlq: 0 })

  useEffect(() => {
    const id = setInterval(() => {
      const s = stream.snapshot
      const f = s.frame
      if (!f) return
      const open = f.endpoints.filter((e) => e.circuit !== 'closed')
      setSummary(
        `Ingest ${s.rates.ingested.toFixed(0)} events per second. Queue depth ${f.totals.pending}. ` +
          `${f.pool.busy} of ${f.pool.workers} workers busy. ${f.endpoints.length} endpoints, ` +
          `${open.length} with an open circuit. Dead-letter queue ${f.totals.dlq}.`,
      )
      const msgs: string[] = []
      for (const e of open)
        if (!prev.current.open.has(e.id)) msgs.push(`Circuit opened for ${e.name}.`)
      for (const id of prev.current.open)
        if (!open.some((e) => e.id === id)) msgs.push(`A circuit closed.`)
      if (Math.floor(f.totals.dlq / 50) > Math.floor(prev.current.dlq / 50))
        msgs.push(`Dead-letter queue passed ${Math.floor(f.totals.dlq / 50) * 50}.`)
      prev.current = { open: new Set(open.map((e) => e.id)), dlq: f.totals.dlq }
      if (msgs.length) setAnnounce(msgs.join(' '))
    }, 1000)
    return () => clearInterval(id)
  }, [])

  return (
    <>
      <div role="img" aria-label={summary} className="sr-only" />
      <div aria-live="polite" className="sr-only">
        {announce}
      </div>
    </>
  )
}
