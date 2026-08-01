import { STATUS_DESCRIPTIONS, STATUS_LABELS, type Status } from '@/lib/status'
import { cn } from '@/lib/utils'

/**
 * The identity of the product.
 *
 * Not a generic Badge. Every pill carries a SHAPE GLYPH alongside its color,
 * which does two jobs at once:
 *
 *   1. Accessibility. Status is never encoded by color alone, so the six
 *      states stay distinguishable under protanopia and deuteranopia — where
 *      `failed` red and `dead` violet, or `retrying` amber and `delivered`
 *      green, otherwise start to converge.
 *   2. Identity. A hollow ring for "queued" and a ✕-in-a-ring for "dead" are
 *      what make the component look designed rather than themed.
 *
 * The glyphs are chosen to mean something rather than to decorate:
 *   queued      hollow ring          nothing spent yet
 *   delivering  dot with a trail     motion
 *   delivered   filled dot           done
 *   retrying    notched half-ring    partial, cycling
 *   failed      ✕                    this attempt was lost
 *   dead        ✕ inside a ring      terminal, and contained
 */

function Glyph({ status, className }: { status: Status; className?: string }) {
  const common = {
    width: 8,
    height: 8,
    viewBox: '0 0 8 8',
    fill: 'none',
    'aria-hidden': true as const,
    className,
  }
  const c = 'currentColor'

  switch (status) {
    case 'queued':
      return (
        <svg {...common}>
          <circle cx="4" cy="4" r="2.5" stroke={c} strokeWidth="1.25" />
        </svg>
      )
    case 'delivering':
      return (
        <svg {...common}>
          <circle cx="5" cy="4" r="2" fill={c} />
          <path d="M0.5 4H1.75" stroke={c} strokeWidth="1.25" strokeLinecap="round" />
        </svg>
      )
    case 'delivered':
      return (
        <svg {...common}>
          <circle cx="4" cy="4" r="2.5" fill={c} />
        </svg>
      )
    case 'retrying':
      return (
        <svg {...common}>
          {/* Half-filled and notched: partial progress, going around again. */}
          <path d="M4 1.5a2.5 2.5 0 010 5z" fill={c} />
          <path d="M4 1.5a2.5 2.5 0 000 5" stroke={c} strokeWidth="1.1" strokeLinecap="round" />
        </svg>
      )
    case 'failed':
      return (
        <svg {...common}>
          <path
            d="M1.75 1.75l4.5 4.5M6.25 1.75l-4.5 4.5"
            stroke={c}
            strokeWidth="1.25"
            strokeLinecap="round"
          />
        </svg>
      )
    case 'dead':
      return (
        <svg {...common}>
          <circle cx="4" cy="4" r="3.2" stroke={c} strokeWidth="1" />
          <path
            d="M2.6 2.6l2.8 2.8M5.4 2.6L2.6 5.4"
            stroke={c}
            strokeWidth="1.1"
            strokeLinecap="round"
          />
        </svg>
      )
  }
}

export interface StatusPillProps {
  status: Status
  /** Appends the retry position, e.g. "· 3/8". */
  attempt?: { n: number; max: number }
  /** `soft` in tables (default), `solid` for canvas nodes and legends. */
  tone?: 'soft' | 'solid' | 'bare'
  size?: 'xs' | 'sm'
  /** Adds a slow pulse on the glyph only — never on the whole pill. */
  live?: boolean
  className?: string
}

export function StatusPill({
  status,
  attempt,
  tone = 'soft',
  size = 'xs',
  live = false,
  className,
}: StatusPillProps) {
  // color-mix against the surface keeps the tint consistent in both themes
  // without maintaining a second set of twelve tokens.
  const style =
    tone === 'solid'
      ? {
          color: 'var(--bg-surface)',
          backgroundColor: `var(--st-${status})`,
          borderColor: `var(--st-${status})`,
        }
      : tone === 'bare'
        ? {
            color: `var(--st-${status})`,
            backgroundColor: 'transparent',
            borderColor: 'transparent',
          }
        : {
            color: `var(--st-${status}-ink)`,
            backgroundColor: `color-mix(in oklch, var(--st-${status}) 16%, var(--bg-surface))`,
            borderColor: `color-mix(in oklch, var(--st-${status}) 32%, transparent)`,
          }

  return (
    <span
      data-status={status}
      style={style}
      title={STATUS_DESCRIPTIONS[status]}
      className={cn(
        'inline-flex select-none items-center gap-1.5 whitespace-nowrap rounded-[4px] border font-mono',
        't-micro',
        size === 'xs' ? 'h-5 px-1.5' : 'h-6 px-2',
        className,
      )}
    >
      <Glyph status={status} className={cn('shrink-0', live && 'animate-pulse')} />
      <span>{STATUS_LABELS[status]}</span>
      {attempt && (
        <span className="opacity-70" data-numeric>
          · {attempt.n}/{attempt.max}
        </span>
      )}
      {/* The visible label reads fine, but the meaning is not obvious out of
          context — so the full definition is available to a screen reader. */}
      <span className="sr-only">. {STATUS_DESCRIPTIONS[status]}</span>
    </span>
  )
}
