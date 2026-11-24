import 'uplot/dist/uPlot.min.css'

import { useEffect, useRef } from 'react'
import uPlot, { type AlignedData, type Options } from 'uplot'

import { useTheme } from '@/lib/theme-context'

export interface Series {
  label: string
  /** a CSS variable name, read at draw time so a theme switch is free */
  color: string
  /** right axis (latency) instead of left (throughput) */
  right?: boolean
  fill?: boolean
  dash?: number[]
}

interface TimeSeriesProps {
  data: AlignedData // [timestamps (s), ...series]
  series: Series[]
  height?: number
  leftUnit?: string
  rightUnit?: string
  label: string
}

/**
 * The ONE place that knows uPlot. Canvas-based, so redrawing thousands of
 * points costs nothing and never re-renders React. Colors come from CSS
 * variables at draw time, so themes need no chart code.
 *
 * No tweening between datasets: a chart that animates on every 10s refetch is
 * decoration, and decoration on a monitoring screen is noise.
 */
export function TimeSeries({
  data,
  series,
  height = 180,
  leftUnit = '',
  rightUnit = '',
  label,
}: TimeSeriesProps) {
  const el = useRef<HTMLDivElement>(null)
  const plot = useRef<uPlot | null>(null)
  const { resolvedTheme } = useTheme()

  useEffect(() => {
    if (!el.current) return
    const css = getComputedStyle(document.documentElement)
    const v = (name: string) => css.getPropertyValue(name).trim()
    const axis = (unit: string, side: 0 | 1, scale: string): uPlot.Axis => ({
      scale,
      side: side === 0 ? 3 : 1,
      stroke: v('--fg-4'),
      grid: { show: side === 0, stroke: v('--grid'), width: 1 },
      ticks: { show: false },
      font: '10px "Geist Mono Variable", monospace',
      size: 44,
      values: (_u, vals) =>
        vals.map((x) => (x >= 1000 ? `${(x / 1000).toFixed(1)}k` : String(Math.round(x))) + unit),
    })
    const opts: Options = {
      width: el.current.clientWidth,
      height,
      legend: { show: false },
      cursor: { points: { size: 6 }, y: false },
      scales: {
        x: { time: true },
        y: { range: (_u, _min, max) => [0, Math.max(1, max * 1.15)] },
        ms: { range: (_u, _min, max) => [0, Math.max(10, max * 1.15)] },
      },
      axes: [
        {
          stroke: v('--fg-4'),
          grid: { show: false },
          ticks: { show: false },
          font: '10px "Geist Mono Variable", monospace',
        },
        axis(leftUnit, 0, 'y'),
        ...(series.some((s) => s.right) ? [axis(rightUnit, 1, 'ms')] : []),
      ],
      series: [
        {},
        ...series.map((s) => ({
          label: s.label,
          scale: s.right ? 'ms' : 'y',
          stroke: v(s.color),
          width: 1.5,
          dash: s.dash,
          fill: s.fill ? `color-mix(in oklch, ${v(s.color)} 14%, transparent)` : undefined,
          points: { show: false },
        })),
      ],
    }
    plot.current = new uPlot(opts, data, el.current)
    const ro = new ResizeObserver(
      () => el.current && plot.current?.setSize({ width: el.current.clientWidth, height }),
    )
    ro.observe(el.current)
    return () => {
      ro.disconnect()
      plot.current?.destroy()
      plot.current = null
    }
    // Rebuild on theme change (colors are read at build time) or series shape change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resolvedTheme, series.length, height])

  useEffect(() => {
    plot.current?.setData(data)
  }, [data])

  return <div ref={el} role="img" aria-label={label} className="w-full" />
}
