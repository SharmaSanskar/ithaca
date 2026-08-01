import { createContext, use } from 'react'

export type Theme = 'dark' | 'light' | 'system'
export type Density = 'compact' | 'comfortable'

export const THEME_KEY = 'ithaca.theme'
export const DENSITY_KEY = 'ithaca.density'

export interface ThemeContextValue {
  theme: Theme
  resolvedTheme: 'dark' | 'light'
  setTheme: (t: Theme) => void
  density: Density
  setDensity: (d: Density) => void
}

export const ThemeContext = createContext<ThemeContextValue | null>(null)

export function useTheme(): ThemeContextValue {
  const ctx = use(ThemeContext)
  if (!ctx) throw new Error('useTheme must be used inside <ThemeProvider>')
  return ctx
}
