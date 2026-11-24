/**
 * The particle engine: everything that moves on the flow canvas.
 *
 * It lives entirely outside React. A rAF loop reads the React Flow viewport
 * and pre-sampled edge geometry, and draws onto one <canvas>. Particle state
 * is struct-of-arrays in typed arrays with a free list: zero allocation in the
 * hot loop, so 60fps is real rather than aspirational.
 *
 * THE HONESTY RULES (docs/design/dashboard.md §3.2):
 *
 *   - The particle count is a SAMPLE; the number on the edge label is the
 *     truth. An edge emits at most CAP_PER_SEC particles; each particle stands
 *     for `weight` real events, and the label says so (~1:N).
 *   - Fractional emission uses a token bucket, so a 0.3-per-frame edge emits
 *     one particle every ~3 frames instead of rounding down to nothing.
 *   - A failure is NEVER sampled out: if an edge saw one, a failure particle
 *     is emitted, however few particles the cap allows.
 *   - Speed encodes LATENCY and nothing else. A slow endpoint's particles
 *     visibly crawl; rate never changes speed.
 */
import type { Frame } from '@/lib/stream'

export const LUT_POINTS = 64
const MAX_PARTICLES = 1200
const CAP_PER_SEC = 12
const DENSITY_THRESHOLD = 200 // events/s above which an edge becomes a flow band

/** Status palette indices, resolved to CSS colors on theme change. */
export const ST = {
  queued: 0,
  delivering: 1,
  delivered: 2,
  retrying: 3,
  failed: 4,
  dead: 5,
  signal: 6,
} as const
const ST_VARS = [
  '--st-queued',
  '--st-delivering',
  '--st-delivered',
  '--st-retrying',
  '--st-failed',
  '--st-dead',
  '--signal',
]

export interface EdgeGeom {
  id: string
  lut: Float32Array // x,y pairs in flow coordinates
  path: Path2D
}

/** What one server frame says about one edge: counts by status. */
export interface EdgeEmission {
  edge: string
  counts: Partial<Record<keyof typeof ST, number>>
  /** traversal seconds (latency-mapped for endpoint edges) */
  duration: number
  /** "dead" edges recolor from failed to dead at the midpoint */
  dying?: boolean
}

export type Viewport = { x: number; y: number; zoom: number }

export class ParticleEngine {
  private ctx: CanvasRenderingContext2D
  private edges: EdgeGeom[] = []
  private edgeIndex = new Map<string, number>()
  private colors: string[] = []

  // struct-of-arrays particle state
  private edge = new Int16Array(MAX_PARTICLES)
  private t = new Float32Array(MAX_PARTICLES)
  private speed = new Float32Array(MAX_PARTICLES) // progress per second
  private status = new Uint8Array(MAX_PARTICLES)
  private radius = new Float32Array(MAX_PARTICLES)
  private dying = new Uint8Array(MAX_PARTICLES)
  private alive = new Uint8Array(MAX_PARTICLES)
  private free: number[] = []
  private live = 0

  private tokens = new Map<string, number>() // token bucket per edge+status
  private rate = new Map<string, number>() // events/s per edge, smoothed
  private raf = 0
  private last = 0
  private dashOffset = 0
  sampled = false

  constructor(
    private canvas: HTMLCanvasElement,
    private viewport: () => Viewport,
    private reducedMotion: () => boolean,
  ) {
    const ctx = canvas.getContext('2d')
    if (!ctx) throw new Error('2d canvas unavailable')
    this.ctx = ctx
    for (let i = MAX_PARTICLES - 1; i >= 0; i--) this.free.push(i)
    this.refreshColors()
  }

  refreshColors() {
    const css = getComputedStyle(document.documentElement)
    this.colors = ST_VARS.map((v) => css.getPropertyValue(v).trim())
  }

  setEdges(edges: EdgeGeom[]) {
    this.edges = edges
    this.edgeIndex = new Map(edges.map((e, i) => [e.id, i]))
    // Geometry changed: drop particles on edges that no longer exist.
    for (let i = 0; i < MAX_PARTICLES; i++) {
      if (this.alive[i] && this.edge[i] >= edges.length) this.kill(i)
    }
  }

  /** Feed one 100ms server frame's worth of emissions. */
  emit(emissions: EdgeEmission[], windowMs: number) {
    const perFrameCap = (CAP_PER_SEC * windowMs) / 1000
    for (const em of emissions) {
      const idx = this.edgeIndex.get(em.edge)
      if (idx === undefined) continue
      const total = Object.values(em.counts).reduce((a, b) => a + (b ?? 0), 0)
      const r = (total * 1000) / windowMs
      this.rate.set(em.edge, (this.rate.get(em.edge) ?? r) * 0.8 + r * 0.2)
      if (total === 0 || this.reducedMotion()) continue

      const dense = (this.rate.get(em.edge) ?? 0) > DENSITY_THRESHOLD
      const budget = dense ? perFrameCap / 4 : perFrameCap // dense edges: a few "flavor" particles
      const emitTotal = Math.min(total, budget)
      const weight = total / Math.max(emitTotal, 1)

      for (const [name, n] of Object.entries(em.counts)) {
        if (!n) continue
        const st = ST[name as keyof typeof ST]
        const key = em.edge + ':' + name
        // Stratified: each status gets its proportional share of the budget…
        let tokens = (this.tokens.get(key) ?? 0) + (n / total) * emitTotal
        // …and a failure always gets at least one particle.
        const isFailure = st === ST.failed || st === ST.dead || st === ST.retrying
        if (isFailure && tokens < 1) tokens = 1
        while (tokens >= 1) {
          this.spawn(idx, st, em.duration, weight, em.dying === true)
          tokens -= 1
        }
        this.tokens.set(key, tokens)
      }
    }
  }

  private spawn(edge: number, st: number, duration: number, weight: number, dying: boolean) {
    const i = this.free.pop()
    if (i === undefined) {
      this.sampled = true // over budget: say so on screen
      return
    }
    this.alive[i] = 1
    this.edge[i] = edge
    // Stagger start positions slightly so a frame's batch doesn't move as a clump.
    this.t[i] = Math.random() * 0.08
    this.speed[i] = 1 / Math.max(duration, 0.2)
    this.status[i] = st
    this.radius[i] = Math.min(5, 2 + Math.log2(Math.max(weight, 1)) * 0.6)
    this.dying[i] = dying ? 1 : 0
    this.live++
  }

  private kill(i: number) {
    this.alive[i] = 0
    this.free.push(i)
    this.live--
  }

  start() {
    const loop = (now: number) => {
      this.raf = requestAnimationFrame(loop)
      const dt = this.last ? Math.min((now - this.last) / 1000, 0.1) : 0
      this.last = now
      this.step(dt)
      this.draw()
    }
    this.raf = requestAnimationFrame(loop)
  }

  stop() {
    cancelAnimationFrame(this.raf)
  }

  get particleCount() {
    return this.live
  }

  private step(dt: number) {
    this.dashOffset -= dt * 60 // texture only: speed encodes nothing in density mode
    for (let i = 0; i < MAX_PARTICLES; i++) {
      if (!this.alive[i]) continue
      this.t[i] += this.speed[i] * dt
      if (this.t[i] >= 1) this.kill(i)
    }
    if (this.live < MAX_PARTICLES * 0.8) this.sampled = false
  }

  private draw() {
    const { canvas, ctx } = this
    const dpr = window.devicePixelRatio || 1
    const w = canvas.clientWidth
    const h = canvas.clientHeight
    if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
      canvas.width = Math.round(w * dpr)
      canvas.height = Math.round(h * dpr)
    }
    ctx.setTransform(1, 0, 0, 1, 0, 0)
    ctx.clearRect(0, 0, canvas.width, canvas.height)
    const vp = this.viewport()
    // Same transform as the React Flow viewport: particles pan and zoom with
    // the graph for free.
    ctx.setTransform(dpr * vp.zoom, 0, 0, dpr * vp.zoom, dpr * vp.x, dpr * vp.y)

    const reduced = this.reducedMotion()
    // Flow bands: dense edges (or every active edge under reduced motion,
    // drawn static) show a distribution, not individuals. Under reduced
    // motion the band is QUANTIZED to a few levels of the rate's order of
    // magnitude, so it changes when traffic genuinely changes and is
    // otherwise perfectly still.
    for (const e of this.edges) {
      const r = this.rate.get(e.id) ?? 0
      if (r <= 0.05 || (!reduced && r <= DENSITY_THRESHOLD)) continue
      const level = reduced ? Math.min(4, Math.ceil(Math.log10(1 + r) * 2)) : 0
      ctx.save()
      ctx.globalAlpha = reduced ? 0.15 + level * 0.1 : 0.45
      ctx.strokeStyle = this.colors[ST.delivering]
      ctx.lineWidth = reduced ? 1 + level : Math.min(6, 1.5 + Math.log10(1 + r) * 1.5)
      if (!reduced) {
        ctx.setLineDash([6, 8])
        ctx.lineDashOffset = this.dashOffset
      }
      ctx.stroke(e.path)
      ctx.restore()
    }
    if (reduced) return

    for (let i = 0; i < MAX_PARTICLES; i++) {
      if (!this.alive[i]) continue
      const e = this.edges[this.edge[i]]
      if (!e) continue
      const f = this.t[i] * (LUT_POINTS - 1)
      const k = Math.min(Math.floor(f), LUT_POINTS - 2)
      const u = f - k
      const x = e.lut[k * 2] + (e.lut[k * 2 + 2] - e.lut[k * 2]) * u
      const y = e.lut[k * 2 + 1] + (e.lut[k * 2 + 3] - e.lut[k * 2 + 1]) * u
      // On the path to the DLQ, a failed attempt becomes a dead letter at the
      // midpoint: red turns violet.
      const st = this.dying[i] && this.t[i] > 0.5 ? ST.dead : this.status[i]
      ctx.fillStyle = this.colors[st]
      ctx.beginPath()
      ctx.arc(x, y, this.radius[i], 0, Math.PI * 2)
      ctx.fill()
    }
  }
}

/** Sample an SVG path once into a fixed-size lookup table. */
export function sampleEdge(id: string, el: SVGPathElement): EdgeGeom {
  const len = el.getTotalLength()
  const lut = new Float32Array(LUT_POINTS * 2)
  for (let i = 0; i < LUT_POINTS; i++) {
    const p = el.getPointAtLength((i / (LUT_POINTS - 1)) * len)
    lut[i * 2] = p.x
    lut[i * 2 + 1] = p.y
  }
  return { id, lut, path: new Path2D(el.getAttribute('d') ?? '') }
}

/** Latency → traversal time, log-compressed into 0.4s–4s. */
export function latencyDuration(ms: number): number {
  if (!ms) return 0.9
  return 0.4 + Math.min(1, Math.log10(1 + ms) / Math.log10(1 + 5000)) * 3.6
}

/** Turn one server frame into per-edge emissions for the fixed topology. */
export function emissionsFor(f: Frame): EdgeEmission[] {
  const out: EdgeEmission[] = [
    { edge: 'ingest-queue', counts: { queued: f.ingested }, duration: 0.8 },
  ]
  let attempts = 0
  let retries = 0
  let deaths = 0
  for (const e of f.endpoints) {
    attempts += e.ok + e.retry + e.dead + e.rate_limited
    retries += e.retry + e.rate_limited
    deaths += e.dead
    out.push({
      edge: 'workers-' + e.id,
      counts: { delivered: e.ok, failed: e.retry, dead: e.dead, retrying: e.rate_limited },
      duration: latencyDuration(e.p50_ms),
    })
  }
  out.push({ edge: 'queue-workers', counts: { delivering: attempts }, duration: 0.7 })
  out.push({ edge: 'workers-retry', counts: { retrying: retries }, duration: 1.1 })
  out.push({ edge: 'workers-dlq', counts: { failed: deaths }, duration: 1.4, dying: true })
  return out
}
