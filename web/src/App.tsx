import { BrowserRouter, Navigate, Route, Routes } from 'react-router'

import { ThemeProvider } from '@/lib/theme'
import TokenPalette from '@/routes/TokenPalette'

/**
 * Phase 0 routing: the design-system proof sheet and nothing else.
 *
 * BrowserRouter (not HashRouter) because the Go handler falls back to
 * index.html for any unmatched path, so real URLs deep-link correctly.
 */
export function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/design" element={<TokenPalette />} />
          <Route path="*" element={<Navigate to="/design" replace />} />
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  )
}
