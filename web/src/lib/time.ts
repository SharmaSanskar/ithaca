/** "4s ago", "3m ago" — for feeds and tables where recency matters more than the clock. */
export function ago(t: number | string, now = Date.now()): string {
  const ms = now - (typeof t === 'number' ? t : new Date(t).getTime())
  if (ms < 1000) return 'now'
  if (ms < 60_000) return `${Math.floor(ms / 1000)}s ago`
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ago`
  if (ms < 86_400_000) return `${Math.floor(ms / 3_600_000)}h ago`
  return `${Math.floor(ms / 86_400_000)}d ago`
}

export function clock(t: string | number): string {
  return new Date(t).toLocaleTimeString('en-GB', { hour12: false })
}
