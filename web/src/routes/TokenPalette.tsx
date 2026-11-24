import { Contrast, Monitor, Moon, Rows3, Sun } from 'lucide-react'

import { StatusPill } from '@/components/StatusPill'
import { STATUSES, type Status } from '@/lib/status'
import { useTheme, type Theme } from '@/lib/theme-context'
import { cn, formatCount, formatMs, formatPct } from '@/lib/utils'

/**
 * The design-system proof sheet.
 *
 * This page exists to be looked at side by side with the reference bar
 * (Vercel, Axiom, Linear) and answer one question honestly: does this read as
 * designed, or as a component library with the default theme? It is the
 * Phase 0 visual deliverable and it stays in the app afterwards as living
 * documentation — a token that drifts shows up here first.
 */

function Section({
  title,
  note,
  children,
}: {
  title: string
  note?: string
  children: React.ReactNode
}) {
  return (
    <section className="panel p-4">
      <header className="mb-4 flex items-baseline justify-between gap-4 border-b border-line-faint pb-2">
        <h2 className="t-micro font-mono text-fg-3">{title}</h2>
        {note && <p className="t-xs max-w-[52ch] text-right text-fg-3">{note}</p>}
      </header>
      {children}
    </section>
  )
}

function Swatch({ token, label, hint }: { token: string; label: string; hint?: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <div
        className="h-11 rounded-md border border-line"
        style={{ background: `var(${token})` }}
        aria-hidden
      />
      <div className="min-w-0">
        <div className="t-xs truncate font-mono text-fg-2">{label}</div>
        {hint && <div className="t-micro truncate text-fg-3">{hint}</div>}
      </div>
    </div>
  )
}

export default function TokenPalette() {
  const { theme, setTheme, resolvedTheme, density, setDensity } = useTheme()

  const themes: { value: Theme; icon: typeof Sun; label: string }[] = [
    { value: 'light', icon: Sun, label: 'Light' },
    { value: 'dark', icon: Moon, label: 'Dark' },
    { value: 'system', icon: Monitor, label: 'System' },
  ]

  return (
    <div className="min-h-dvh bg-canvas text-fg">
      {/* ── top bar ─────────────────────────────────────────────────────── */}
      <header className="sticky top-0 z-10 flex h-11 items-center gap-4 border-b border-line bg-canvas/85 px-4 backdrop-blur">
        <div className="flex items-baseline gap-2">
          <span className="t-md font-mono font-medium tracking-tight">ithaca</span>
          <span className="t-micro font-mono text-fg-3">design system</span>
        </div>

        <div className="ml-auto flex items-center gap-1.5">
          <div className="flex items-center gap-0.5 rounded-md border border-line bg-surface p-0.5">
            {themes.map(({ value, icon: Icon, label }) => (
              <button
                key={value}
                type="button"
                onClick={() => setTheme(value)}
                aria-pressed={theme === value}
                title={label}
                className={cn(
                  'flex h-6 w-7 items-center justify-center rounded-[4px] transition-colors',
                  theme === value ? 'bg-hover text-fg' : 'text-fg-3 hover:bg-hover hover:text-fg-2',
                )}
              >
                <Icon size={13} strokeWidth={1.75} aria-hidden />
                <span className="sr-only">{label} theme</span>
              </button>
            ))}
          </div>

          <button
            type="button"
            onClick={() => setDensity(density === 'compact' ? 'comfortable' : 'compact')}
            className="flex h-7 items-center gap-1.5 rounded-md border border-line bg-surface px-2 text-fg-3 transition-colors hover:bg-hover hover:text-fg-2"
            title="Toggle row density"
          >
            <Rows3 size={13} strokeWidth={1.75} aria-hidden />
            <span className="t-micro font-mono">{density}</span>
          </button>
        </div>
      </header>

      <main className="mx-auto max-w-6xl space-y-4 p-4">
        {/* ── the thesis ───────────────────────────────────────────────── */}
        <section className="panel p-5">
          <p className="t-lg max-w-[62ch] text-fg">Color is a data channel, not decoration.</p>
          <p className="t-base mt-2 max-w-[72ch] text-fg-3">
            Every pixel of chrome is achromatic. The only saturated color in the product is{' '}
            <span className="text-fg-2">event state</span> — which is why the six status hues below
            are the entire palette, and why nothing else competes with them for attention.
          </p>
          <p className="t-xs mt-3 font-mono text-fg-3">
            resolved theme: {resolvedTheme} · density: {density} · base font-size: 13px
          </p>
        </section>

        {/* ── status ───────────────────────────────────────────────────── */}
        <Section
          title="Status ramp"
          note="Never color alone: every state carries a shape glyph and a text label, so the six stay distinguishable under protanopia and deuteranopia."
        >
          <div className="flex flex-wrap gap-2">
            {STATUSES.map((s) => (
              <StatusPill key={s} status={s} size="sm" live={s === 'delivering'} />
            ))}
          </div>

          <div className="mt-4 flex flex-wrap gap-2">
            <StatusPill status="retrying" attempt={{ n: 3, max: 8 }} />
            <StatusPill status="delivered" tone="solid" />
            <StatusPill status="dead" tone="solid" />
            <StatusPill status="failed" tone="bare" />
          </div>

          <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
            {STATUSES.map((s: Status) => (
              <Swatch key={s} token={`--st-${s}`} label={`--st-${s}`} />
            ))}
          </div>

          <p className="t-xs mt-4 max-w-[74ch] text-fg-3">
            Violet for <span className="font-mono text-fg-3">dead</span>, not a darker red. The
            dead-letter queue is a different destination, not a worse failure — and two reds
            collapse into one under the most common color-vision deficiencies.
          </p>
        </Section>

        {/* ── surfaces ─────────────────────────────────────────────────── */}
        <Section
          title="Surfaces & hairlines"
          note="Depth is a surface step plus one 1px rule. Nothing below an overlay gets a box-shadow."
        >
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
            <Swatch token="--bg-canvas" label="--bg-canvas" hint="level 0 · page" />
            <Swatch token="--bg-surface" label="--bg-surface" hint="level 1 · panel" />
            <Swatch token="--bg-inset" label="--bg-inset" hint="wells, thead" />
            <Swatch token="--bg-hover" label="--bg-hover" hint="level 2 · hover" />
            <Swatch token="--bg-raised" label="--bg-raised" hint="level 3 · overlay" />
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-3">
            <div className="panel px-3 py-2">
              <span className="t-xs font-mono text-fg-2">.panel</span>
            </div>
            <div className="overlay px-3 py-2">
              <span className="t-xs font-mono text-fg-2">.overlay — the only shadow</span>
            </div>
          </div>
        </Section>

        {/* ── text + accent ────────────────────────────────────────────── */}
        <div className="grid gap-4 lg:grid-cols-2">
          <Section title="Text hierarchy" note="Four levels. The fourth is decorative only.">
            <div className="space-y-1.5">
              <p className="t-base text-fg">--fg · primary · 18.1:1</p>
              <p className="t-base text-fg-2">--fg-2 · secondary · 9.8:1</p>
              <p className="t-base text-fg-3">--fg-3 · muted · 5.4:1</p>
              <p className="t-base text-fg-3">--fg-4 · decorative / large text only · 3.3:1</p>
            </div>
            <div className="mt-4 flex items-center gap-2">
              <Contrast size={13} className="text-fg-3" aria-hidden />
              <span className="t-xs text-fg-3">
                Contrast ratios hold in both themes; --fg-4 never carries body copy.
              </span>
            </div>
          </Section>

          <Section
            title="Signal & quantitative ramp"
            note="One accent hue. The ordinal ramp is one hue with monotone lightness, so the ordering is visible in the color itself."
          >
            <div className="grid grid-cols-4 gap-3">
              <Swatch token="--signal" label="--signal" hint="links, focus" />
              <Swatch token="--q-1" label="--q-1" hint="p50" />
              <Swatch token="--q-2" label="--q-2" hint="p90" />
              <Swatch token="--q-3" label="--q-3" hint="p99" />
            </div>
            <p className="t-xs mt-4 text-fg-3">
              Ithaca ships zero categorical palettes. Comparing endpoints is done with small
              multiples on a shared domain, never eight hues on one chart.
            </p>
          </Section>
        </div>

        {/* ── type ─────────────────────────────────────────────────────── */}
        <Section
          title="Type scale"
          note="Geist and Geist Mono, self-hosted. Base is 13px, not 16px — that one number does most of the work of reading as an instrument."
        >
          <div className="space-y-2">
            {(
              [
                ['t-hero', 'hero metric', 'font-mono'],
                ['t-metric', 'stat tile value', 'font-mono'],
                ['t-lg', 'page title', ''],
                ['t-md', 'panel title', ''],
                ['t-base', 'body and primary table rows', ''],
                ['t-sm', 'dense table body', ''],
                ['t-xs', 'timestamps and help text', ''],
                ['t-micro', 'column heads, pills, axis ticks', 'font-mono'],
              ] as const
            ).map(([cls, use, extra]) => (
              <div key={cls} className="flex items-baseline gap-4 border-b border-line-faint pb-2">
                <code className="t-micro w-20 shrink-0 font-mono text-fg-3">{cls}</code>
                <span className={cn(cls, extra, 'text-fg')}>1,284 ev/s</span>
                <span className="t-xs ml-auto text-fg-3">{use}</span>
              </div>
            ))}
          </div>
        </Section>

        {/* ── numerals ─────────────────────────────────────────────────── */}
        <Section
          title="Tabular numerals"
          note="Every digit in the product is monospace and tabular, so a counter ticking at 10Hz never reflows and columns align on the decimal with no alignment hack."
        >
          <div className="overflow-hidden rounded-md border border-line">
            <table className="w-full">
              <thead>
                <tr className="border-b border-line bg-inset">
                  {['endpoint', 'delivered', 'success', 'p50', 'p99', 'status'].map((h) => (
                    <th
                      key={h}
                      className="t-micro px-3 py-2 text-left font-mono font-normal text-fg-3"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(
                  [
                    ['acme-prod', 1_284_991, 0.9994, 11.4, 142, 'delivered'],
                    ['beta-staging', 48_120, 0.9712, 8.2, 1_240, 'retrying'],
                    ['gamma-eu', 7, 0.4286, 0.42, 30_000, 'failed'],
                    ['delta-legacy', 912_004, 0.0, 0, 0, 'dead'],
                  ] as const
                ).map(([name, delivered, success, p50, p99, status]) => (
                  <tr
                    key={name}
                    className="border-b border-line-faint last:border-0 hover:bg-hover"
                    style={{ height: 'var(--row-h)' }}
                  >
                    <td className="t-sm px-3 font-mono text-fg-2">{name}</td>
                    <td className="t-sm px-3 text-right font-mono text-fg" data-numeric>
                      {formatCount(delivered, false)}
                    </td>
                    <td className="t-sm px-3 text-right font-mono text-fg" data-numeric>
                      {formatPct(success)}
                    </td>
                    <td className="t-sm px-3 text-right font-mono text-fg-2" data-numeric>
                      {formatMs(p50)}
                    </td>
                    <td className="t-sm px-3 text-right font-mono text-fg-2" data-numeric>
                      {formatMs(p99)}
                    </td>
                    <td className="px-3">
                      <StatusPill status={status as Status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="t-xs mt-3 text-fg-3">
            Thousands are separated with a thin space, not a comma — in a tabular column a comma
            sits on the baseline and clumps the digits into a blob.
          </p>
        </Section>

        <footer className="pb-8 pt-2">
          <p className="t-xs font-mono text-fg-3">
            phase 0 · foundation. the delivery engine, live canvas and demo console follow.
          </p>
        </footer>
      </main>
    </div>
  )
}
