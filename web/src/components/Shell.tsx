import { Moon, Sun } from 'lucide-react'
import type { ReactNode } from 'react'
import { NavLink } from 'react-router'

import { useStream } from '@/lib/stream'
import { useTheme } from '@/lib/theme-context'
import { cn } from '@/lib/utils'

const nav = [
  { to: '/', label: 'Mission control', end: true },
  { to: '/demo', label: 'Demo console' },
  { to: '/events', label: 'Events' },
  { to: '/endpoints', label: 'Endpoints' },
  { to: '/dlq', label: 'Dead letters' },
]

export function Shell({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-h-dvh flex-col">
      <header className="sticky top-0 z-20 flex h-11 shrink-0 items-center gap-6 border-b border-line bg-canvas/90 px-4 backdrop-blur">
        <NavLink to="/" className="flex items-center gap-2" aria-label="Ithaca home">
          <Logo />
          <span className="t-md font-semibold tracking-tight text-fg">ithaca</span>
        </NavLink>
        <nav className="flex items-center gap-1" aria-label="Main">
          {nav.map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              end={n.end}
              className={({ isActive }) =>
                cn(
                  'rounded-[4px] px-2 py-1 t-sm text-fg-3 transition-colors hover:text-fg',
                  isActive && 'bg-hover text-fg',
                )
              }
            >
              {n.label}
            </NavLink>
          ))}
        </nav>
        <div className="ml-auto flex items-center gap-3">
          <ConnectionBadge />
          <ThemeToggle />
          <a
            href="https://github.com/SharmaSanskar/ithaca"
            className="t-sm text-fg-3 hover:text-fg"
            target="_blank"
            rel="noreferrer"
          >
            GitHub
          </a>
        </div>
      </header>
      <main className="flex min-h-0 flex-1 flex-col p-4">{children}</main>
    </div>
  )
}

function Logo() {
  // Three nodes on a line, one dropping below it: the pipeline and its DLQ.
  return (
    <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden>
      <path d="M2 7h14" stroke="var(--line-strong)" strokeWidth="1.25" />
      <path d="M11 7l3 7" stroke="var(--st-dead)" strokeWidth="1.25" />
      <circle cx="3" cy="7" r="2" fill="var(--signal)" />
      <circle cx="9" cy="7" r="2" fill="var(--signal)" />
      <circle cx="15" cy="7" r="2" fill="var(--signal)" />
      <circle cx="14" cy="14" r="2" fill="var(--st-dead)" />
    </svg>
  )
}

function ConnectionBadge() {
  const connection = useStream((s) => s.connection)
  const live = connection === 'live'
  return (
    <span
      className="flex items-center gap-1.5 t-xs font-mono text-fg-3"
      role="status"
      aria-live="polite"
    >
      <span
        className={cn(
          'size-1.5 rounded-full',
          live ? 'animate-pulse bg-st-delivered' : 'bg-st-retrying',
        )}
      />
      {live ? 'live · 10Hz' : connection}
    </span>
  )
}

function ThemeToggle() {
  const { resolvedTheme, setTheme } = useTheme()
  const dark = resolvedTheme === 'dark'
  return (
    <button
      type="button"
      onClick={() => setTheme(dark ? 'light' : 'dark')}
      className="rounded-[4px] p-1 text-fg-3 hover:bg-hover hover:text-fg"
      aria-label={dark ? 'Switch to light theme' : 'Switch to dark theme'}
    >
      {dark ? <Sun size={14} /> : <Moon size={14} />}
    </button>
  )
}
