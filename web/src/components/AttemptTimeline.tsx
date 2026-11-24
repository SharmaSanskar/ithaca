import { useEffect, useState, type KeyboardEvent } from 'react'

import type { Attempt } from '@/lib/api'
import { cn, formatMs } from '@/lib/utils'

interface AttemptTimelineProps {
  createdAt: string
  attempts: Attempt[]
  /** delivery still open: draw the next attempt as a live node with a countdown */
  nextAttemptAt?: string
  /** delivery ended in the DLQ: cap the line with the dead-letter marker */
  dead?: boolean
  selected: number | null
  onSelect: (i: number) => void
}

const outcomeColor: Record<Attempt['outcome'], string> = {
  success: 'var(--st-delivered)',
  retryable: 'var(--st-failed)',
  timeout: 'var(--st-failed)',
  permanent: 'var(--st-failed)',
  rate_limited: 'var(--st-retrying)',
  blocked: 'var(--st-queued)',
}

/**
 * The retry ladder for one delivery, with the backoff gaps drawn TO SCALE on a
 * LOG time axis measured from ingest. A linear axis would crush the first
 * second into a pixel and spend the whole width on the last hour; on a log
 * axis you see the exponential widening, which is the point of the picture.
 *
 *   ingest   try 1    try 2       try 3              try 4
 *     ●───────▣───────▣───────────▣──────────────────▣
 *            503     503         429                200
 */
export function AttemptTimeline({
  createdAt,
  attempts,
  nextAttemptAt,
  dead,
  selected,
  onSelect,
}: AttemptTimelineProps) {
  const t0 = new Date(createdAt).getTime()
  const now = useNow(nextAttemptAt ? 250 : 0)
  const times = attempts.map((a) => Math.max(0, new Date(a.started_at).getTime() - t0))
  const next = nextAttemptAt ? Math.max(0, new Date(nextAttemptAt).getTime() - t0) : null
  const end = Math.max(1, ...times, next ?? 0)

  // log axis: position ∝ log(1 + t/50ms), padded so the last node is inside.
  const pos = (ms: number) => 4 + (Math.log1p(ms / 50) / Math.log1p(end / 50)) * 84
  // Two close times collapse to one spot on a log axis; the next-attempt
  // marker must never sit on top of the attempt before it.
  const lastPos = times.length ? pos(times[times.length - 1]) : 0
  const nextPos = next !== null ? Math.min(96, Math.max(pos(next), lastPos + 7)) : 0

  const onKey = (e: KeyboardEvent) => {
    if (selected === null) return
    if (e.key === 'ArrowRight') onSelect(Math.min(attempts.length - 1, selected + 1))
    if (e.key === 'ArrowLeft') onSelect(Math.max(0, selected - 1))
  }

  return (
    <div
      className="relative h-[68px] select-none"
      role="tablist"
      aria-label="Delivery attempts"
      onKeyDown={onKey}
    >
      {/* the spine */}
      <div className="absolute top-[22px] right-0 left-0 h-px bg-line-strong" />

      {/* ingest */}
      <div className="absolute top-[17px]" style={{ left: '0%' }}>
        <div className="size-[11px] rounded-full border border-fg-3 bg-surface" title="Ingested" />
        <div className="mt-2 -ml-1 t-micro font-mono text-fg-3">ingest</div>
      </div>

      {/* backoff gaps, labelled at their midpoint */}
      {times.slice(1).map((t, i) => {
        const a = pos(times[i])
        const b = pos(t)
        return (
          <div
            key={`gap-${i}`}
            className="absolute top-[4px] flex justify-center"
            style={{ left: `${a}%`, width: `${b - a}%` }}
          >
            <span className="font-mono text-[10px] text-fg-3">
              +{formatMs(t - times[i] - (attempts[i].duration_ms ?? 0))}
            </span>
          </div>
        )
      })}

      {attempts.map((a, i) => (
        <button
          key={a.attempt + '-' + i}
          type="button"
          role="tab"
          aria-selected={selected === i}
          tabIndex={selected === i || (selected === null && i === attempts.length - 1) ? 0 : -1}
          onClick={() => onSelect(i)}
          aria-label={`Attempt ${a.attempt} of ${attempts.length}: ${a.status_code ? 'HTTP ' + a.status_code : (a.error_class ?? a.outcome)}, ${
            a.duration_ms ?? 0
          } milliseconds`}
          className="absolute top-[15px] -translate-x-1/2 text-center"
          style={{ left: `${pos(times[i])}%` }}
        >
          <span
            className={cn(
              'mx-auto block size-[14px] rounded-[3px] border-2 transition-shadow',
              selected === i ? 'border-fg ring-2 ring-[var(--focus-ring)]' : 'border-transparent',
            )}
            style={{ background: outcomeColor[a.outcome] }}
          />
          <span className="mt-1 block t-xs font-mono text-fg-2">
            {a.status_code ?? a.error_class ?? '—'}
          </span>
          <span className="block font-mono text-[10px] text-fg-3">{a.duration_ms ?? 0} ms</span>
        </button>
      ))}

      {next !== null && (
        <div
          className="absolute top-[15px] -translate-x-1/2 text-center"
          style={{ left: `${nextPos}%` }}
          title="Next attempt"
        >
          <span className="mx-auto block size-[14px] animate-pulse rounded-[3px] border-2 border-dashed border-st-retrying" />
          <span className="mt-1 block t-xs font-mono text-st-retrying-ink">
            {next + t0 > now ? `in ${formatMs(next + t0 - now)}` : 'due'}
          </span>
        </div>
      )}

      {dead && (
        <div className="absolute top-[13px] right-0 text-center" title="Dead-lettered">
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden>
            <circle cx="9" cy="9" r="7" fill="none" stroke="var(--st-dead)" strokeWidth="1.5" />
            <path d="M6 6l6 6M12 6l-6 6" stroke="var(--st-dead)" strokeWidth="1.5" />
          </svg>
          <span className="block t-micro font-mono text-st-dead-ink">DLQ</span>
        </div>
      )}
    </div>
  )
}

function useNow(every: number) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!every) return
    const id = setInterval(() => setNow(Date.now()), every)
    return () => clearInterval(id)
  }, [every])
  return now
}
