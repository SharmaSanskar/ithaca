import { useCallback, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'

import { DENSITY_KEY, THEME_KEY, ThemeContext, type Density, type Theme } from '@/lib/theme-context'

/**
 * Theme and density, without a dependency.
 *
 * next-themes exists to solve server-rendering hydration mismatches. This is a
 * client-only SPA, so there is no mismatch to solve — the blocking script in
 * index.html has already stamped the class before React mounts, and this
 * provider only keeps React's idea of the theme in sync with the DOM.
 */

function readStored<T extends string>(key: string, fallback: T, valid: readonly T[]): T {
  try {
    const v = localStorage.getItem(key)
    if (v && (valid as readonly string[]).includes(v)) return v as T
  } catch {
    // Storage is unavailable in some private-browsing modes. The default is fine.
  }
  return fallback
}

function systemPrefersDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(() =>
    readStored<Theme>(THEME_KEY, 'system', ['dark', 'light', 'system']),
  )
  const [density, setDensityState] = useState<Density>(() =>
    readStored<Density>(DENSITY_KEY, 'compact', ['compact', 'comfortable']),
  )
  const [systemDark, setSystemDark] = useState(systemPrefersDark)

  // Follow the OS for as long as the user has not made an explicit choice.
  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    const onChange = (e: MediaQueryListEvent) => setSystemDark(e.matches)
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [])

  const resolvedTheme: 'dark' | 'light' =
    theme === 'system' ? (systemDark ? 'dark' : 'light') : theme

  useEffect(() => {
    const root = document.documentElement
    root.classList.toggle('dark', resolvedTheme === 'dark')
    // color-scheme drives native form controls and scrollbars, which stay
    // stubbornly light otherwise and give the whole page away.
    root.style.colorScheme = resolvedTheme
  }, [resolvedTheme])

  useEffect(() => {
    document.documentElement.dataset.density = density
  }, [density])

  const setTheme = useCallback((t: Theme) => {
    setThemeState(t)
    try {
      if (t === 'system') localStorage.removeItem(THEME_KEY)
      else localStorage.setItem(THEME_KEY, t)
    } catch {
      /* storage unavailable */
    }
  }, [])

  const setDensity = useCallback((d: Density) => {
    setDensityState(d)
    try {
      localStorage.setItem(DENSITY_KEY, d)
    } catch {
      /* storage unavailable */
    }
  }, [])

  const value = useMemo(
    () => ({ theme, resolvedTheme, setTheme, density, setDensity }),
    [theme, resolvedTheme, setTheme, density, setDensity],
  )

  return <ThemeContext value={value}>{children}</ThemeContext>
}
