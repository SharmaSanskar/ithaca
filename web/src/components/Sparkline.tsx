interface SparklineProps {
  points: number[]
  /** A second series drawn as a band underneath, always in the failed color. */
  failures?: number[]
  /**
   * The y-domain. REQUIRED for any table of sparklines: per-row auto-scaling
   * makes a 4/min endpoint look identical to a 4000/min one, which is the
   * most common sparkline lie there is.
   */
  domain: [number, number]
  width?: number
  height?: number
  /** Terminal dot at the latest value. */
  cap?: boolean
  label: string
  color?: string
}

/** Hand-rolled SVG: one path, no chart library in a table cell. */
export function Sparkline({
  points,
  failures,
  domain,
  width = 72,
  height = 20,
  cap = true,
  label,
  color = 'var(--q-2)',
}: SparklineProps) {
  const n = Math.max(points.length, 2)
  const [lo, hi] = domain
  const span = hi - lo || 1
  const x = (i: number) => (i / (n - 1)) * (width - 3) + 1
  const y = (v: number) =>
    height - 1.5 - ((Math.min(Math.max(v, lo), hi) - lo) / span) * (height - 3)

  const line = points
    .map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`)
    .join('')
  const band = failures?.length
    ? `M${x(0)},${height}` +
      failures.map((v, i) => `L${x(i).toFixed(1)},${y(v).toFixed(1)}`).join('') +
      `L${x(failures.length - 1)},${height}Z`
    : null
  const last = points.length ? points[points.length - 1] : null

  return (
    <svg
      width={width}
      height={height}
      role="img"
      aria-label={label}
      className="block overflow-visible"
    >
      {band && <path d={band} fill="var(--st-failed)" opacity={0.35} />}
      {points.length > 1 && (
        <path
          d={line}
          fill="none"
          stroke={color}
          strokeWidth={1.5}
          vectorEffect="non-scaling-stroke"
          strokeLinejoin="round"
        />
      )}
      {cap && last !== null && (
        <circle cx={x(points.length - 1)} cy={y(last)} r={2.5} fill="var(--signal)" />
      )}
    </svg>
  )
}
