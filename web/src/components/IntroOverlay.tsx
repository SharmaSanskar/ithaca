import { ArrowRight, X } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { Button } from './ui'

const KEY = 'ithaca.intro.v1'

function seen(): boolean {
  try {
    return localStorage.getItem(KEY) === '1'
  } catch {
    return false
  }
}

/**
 * First visit only: what am I looking at, in three sentences. A recruiter
 * gives a portfolio link about thirty seconds; this is what makes those
 * seconds land.
 */
export function IntroOverlay({ onStart }: { onStart: () => void }) {
  const [open, setOpen] = useState(() => !seen())
  const ref = useRef<HTMLButtonElement>(null)
  const close = () => {
    try {
      localStorage.setItem(KEY, '1')
    } catch {
      /* storage unavailable: show again next time, which is harmless */
    }
    setOpen(false)
  }
  useEffect(() => {
    if (!open) return
    ref.current?.focus()
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && close()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open])
  if (!open) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-canvas/60 p-4 backdrop-blur-[2px]"
      role="dialog"
      aria-modal="true"
      aria-labelledby="intro-title"
    >
      <div className="overlay w-full max-w-[520px] p-5">
        <div className="flex items-start justify-between">
          <h2 id="intro-title" className="t-lg font-semibold text-fg">
            What you&rsquo;re looking at
          </h2>
          <button
            type="button"
            onClick={close}
            aria-label="Close"
            className="text-fg-3 hover:text-fg"
          >
            <X size={16} />
          </button>
        </div>
        <ol className="mt-3 flex flex-col gap-2.5 t-base text-fg-2">
          <li>
            <span className="text-fg">Ithaca delivers webhooks.</span> Apps send it events; it
            guarantees each one reaches every subscribed endpoint, in order where that matters,
            through crashes, slow servers and outages.
          </li>
          <li>
            <span className="text-fg">The graph is the live system, not an animation.</span> Every
            dot is a real event moving through a real Go backend: ingest → Postgres queue → a
            bounded worker pool → four HTTP endpoints.
          </li>
          <li>
            <span className="text-fg">Break something and watch it cope.</span> Scenarios take
            endpoints down or slow them; retries back off, a circuit breaker opens, failures go to a
            dead-letter queue, and ingest never slows down.
          </li>
        </ol>
        <div className="mt-5 flex items-center gap-2">
          <Button
            ref={ref}
            variant="primary"
            onClick={() => {
              close()
              onStart()
            }}
          >
            Break an endpoint <ArrowRight size={13} />
          </Button>
          <Button variant="ghost" onClick={close}>
            Just explore
          </Button>
        </div>
      </div>
    </div>
  )
}
