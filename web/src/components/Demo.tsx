import { CheckCircle2, CircleAlert, Info, Play, Square } from 'lucide-react'

import { useDemoAction, useDemoState } from '@/lib/api'
import { useStream, type Narration } from '@/lib/stream'
import { cn } from '@/lib/utils'

import { Button } from './ui'

/** Five one-click, SERVER-driven scenarios. The UI only watches, and says so. */
export function ScenarioBar({ compact = false }: { compact?: boolean }) {
  const { data } = useDemoState()
  const act = useDemoAction()
  const running = data?.scenario
  const error = act.error instanceof Error ? act.error.message : null

  return (
    <div className="flex flex-wrap items-center gap-2">
      {!compact && <span className="t-micro font-mono text-fg-3">Run a scenario</span>}
      {(data?.scenarios ?? []).map((s) => {
        const active = running?.name === s.name
        return (
          <Button
            key={s.name}
            size="xs"
            variant={active ? 'primary' : 'secondary'}
            onClick={() => act.mutate({ path: `/scenarios/${s.name}` })}
            title={s.description}
            disabled={act.isPending}
          >
            <Play size={10} />
            {s.title}
            {active && (
              <span className="font-mono opacity-80">
                {running.step}/{running.total}
              </span>
            )}
          </Button>
        )
      })}
      {running && (
        <Button size="xs" variant="ghost" onClick={() => act.mutate({ path: '/stop' })}>
          <Square size={10} /> Stop
        </Button>
      )}
      {data && !data.leader && (
        <span className="t-xs text-st-retrying-ink">demo runtime offline</span>
      )}
      {error && <span className="t-xs text-st-failed-ink">{error}</span>}
      <span className="ml-auto t-xs text-fg-3">
        Scenarios run on the server; this page only observes.
      </span>
    </div>
  )
}

const icon = { info: Info, warn: CircleAlert, success: CheckCircle2 }
const tone = { info: 'text-fg-2', warn: 'text-st-retrying-ink', success: 'text-st-delivered-ink' }

/**
 * The narration rail: the server's plain-English commentary on the scenario
 * that is running, every number in it measured, not scripted.
 */
export function NarrationRail({ max = 6 }: { max?: number }) {
  const lines = useStream((s) => s.narration).filter((n) => n.text)
  if (lines.length === 0) {
    return (
      <p className="t-sm text-fg-3">
        Pick a scenario above. The commentary appears here as it happens.
      </p>
    )
  }
  return (
    <ol className="flex flex-col gap-1.5" aria-label="Scenario narration" aria-live="polite">
      {lines.slice(-max).map((n: Narration, i, arr) => {
        const Icon = icon[n.level]
        const latest = i === arr.length - 1
        return (
          <li
            key={`${n.scenario}-${n.step}-${i}`}
            className={cn('flex gap-2 t-sm', latest ? tone[n.level] : 'text-fg-3')}
          >
            <Icon size={14} className="mt-[2px] shrink-0" aria-hidden />
            <span className="w-8 shrink-0 font-mono t-xs text-fg-3">
              {n.step}/{n.total}
            </span>
            <span
              className={cn(latest && 'text-fg', n.level === 'success' && latest && 'font-medium')}
            >
              {n.text}
            </span>
          </li>
        )
      })}
    </ol>
  )
}
