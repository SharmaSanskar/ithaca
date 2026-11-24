import { useEffect, useRef } from 'react'

import { prefersReducedMotion } from '@/lib/motion'
import { cn, formatCount } from '@/lib/utils'

type Format = 'count' | 'rate' | 'ms' | 'pct'

interface NumProps {
  value: number | null | undefined
  format?: Format
  /** Abbreviate (12.5k) above 10k by default. */
  abbrev?: boolean | 'auto'
  /** Flash the background when the value changes. */
  flash?: boolean
  className?: string
  /** Accessible label override; defaults to the full spoken value. */
  label?: string
}

/**
 * Every number in the app goes through this.
 *
 *   - tabular mono, so a counter ticking at 10Hz never reflows
 *   - thousands grouped with a THIN SPACE, not a comma
 *   - the unit in the dimmest ink, so the eye reads the magnitude first
 *   - null renders an em dash, never 0 ("unknown" and "zero" are different)
 *   - on change: a 240ms background FLASH. No odometer, no count-up: rolling
 *     digits on a 10Hz dashboard are motion sickness; a tint reads instantly
 *     as "this just changed" and costs no layout.
 */
export function Num({
  value,
  format = 'count',
  abbrev = 'auto',
  flash = false,
  className,
  label,
}: NumProps) {
  const ref = useRef<HTMLSpanElement>(null)
  const prev = useRef(value)

  useEffect(() => {
    // element.animate() is NOT covered by the CSS reduced-motion rule, so the
    // preference is checked here explicitly.
    if (!flash || prev.current === value || !ref.current || prefersReducedMotion()) {
      prev.current = value
      return
    }
    prev.current = value
    ref.current.animate(
      [{ backgroundColor: 'var(--signal-wash)' }, { backgroundColor: 'transparent' }],
      {
        duration: 240,
        easing: 'ease-out',
      },
    )
  }, [value, flash])

  if (value === null || value === undefined || Number.isNaN(value)) {
    return (
      <span data-numeric className={cn('text-fg-3', className)} aria-label={label ?? 'unknown'}>
        —
      </span>
    )
  }

  const [text, unit] = render(value, format, abbrev)
  return (
    <span
      ref={ref}
      data-numeric
      className={cn('rounded-[2px] whitespace-nowrap', className)}
      aria-label={label ?? `${text}${unit ? ' ' + unit : ''}`}
    >
      {text}
      {unit && <span className="ml-[2px] text-[0.85em] text-fg-3">{unit}</span>}
    </span>
  )
}

function render(v: number, format: Format, abbrev: boolean | 'auto'): [string, string] {
  switch (format) {
    case 'rate':
      return [v < 10 && v > 0 ? v.toFixed(1) : formatCount(v, abbrev), '/s']
    case 'ms':
      if (v >= 1000) return [(v / 1000).toFixed(v >= 10_000 ? 0 : 1), 's']
      return [v < 10 && v > 0 ? v.toFixed(1) : String(Math.round(v)), 'ms']
    case 'pct':
      return [(v * 100).toFixed(v >= 0.995 || v === 0 ? 0 : 1), '%']
    default:
      return [formatCount(v, abbrev), '']
  }
}
