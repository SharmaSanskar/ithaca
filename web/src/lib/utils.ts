import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

/** Merge class names, with later Tailwind utilities winning over earlier ones. */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Format a count for display.
 *
 * Thousands are separated with a U+2009 THIN SPACE rather than a comma. In a
 * monospace, tabular-numeral column that one substitution is the single
 * cheapest typographic upgrade available — a comma sits on the baseline and
 * visually clumps digits into a blob; a thin space groups them without adding
 * a mark.
 */
export function formatCount(n: number, abbreviate: 'auto' | boolean = 'auto'): string {
  const abbrev = abbreviate === 'auto' ? Math.abs(n) >= 10_000 : abbreviate
  if (abbrev) {
    const units = [
      { v: 1e9, s: 'B' },
      { v: 1e6, s: 'M' },
      { v: 1e3, s: 'k' },
    ]
    for (const { v, s } of units) {
      if (Math.abs(n) >= v) {
        const scaled = n / v
        return `${scaled.toFixed(scaled < 10 ? 1 : 0)}${s}`
      }
    }
  }
  return Math.round(n).toLocaleString('en-US').replace(/,/g, ' ')
}

/**
 * Format a duration in milliseconds.
 *
 * Sub-millisecond values keep a decimal because the ingest path's whole
 * promise is single-digit-millisecond acks — rounding 0.4ms to "0ms" would
 * throw away the number that matters most.
 */
export function formatMs(ms: number): string {
  if (ms < 1) return `${ms.toFixed(2)} ms`
  if (ms < 10) return `${ms.toFixed(1)} ms`
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`
}

/** Format a ratio in [0,1] as a percentage. */
export function formatPct(ratio: number, digits = 2): string {
  return `${(ratio * 100).toFixed(digits)}%`
}
