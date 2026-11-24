import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { lazy, Suspense, useEffect } from 'react'
import { BrowserRouter, Route, Routes } from 'react-router'

import { Shell } from '@/components/Shell'
import { startLive } from '@/lib/live'
import { ThemeProvider } from '@/lib/theme'
import MissionControl from '@/routes/MissionControl'

const Demo = lazy(() => import('@/routes/Demo'))
const Events = lazy(() => import('@/routes/Events'))
const EventDetail = lazy(() => import('@/routes/EventDetail'))
const Endpoints = lazy(() => import('@/routes/Endpoints'))
const DeadLetters = lazy(() => import('@/routes/DeadLetters'))
const TokenPalette = lazy(() => import('@/routes/TokenPalette'))
const NotFound = lazy(() => import('@/routes/NotFound'))

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 5_000, refetchOnWindowFocus: false, retry: 1 },
  },
})

/**
 * BrowserRouter (not HashRouter): the Go handler serves index.html for any
 * unmatched path, so real URLs deep-link correctly.
 */
export function App() {
  useEffect(() => startLive(queryClient), [])
  return (
    <ThemeProvider>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <Shell>
            <Suspense fallback={null}>
              <Routes>
                <Route path="/" element={<MissionControl />} />
                <Route path="/demo" element={<Demo />} />
                <Route path="/events" element={<Events />} />
                <Route path="/events/:id" element={<EventDetail />} />
                <Route path="/endpoints" element={<Endpoints />} />
                <Route path="/dlq" element={<DeadLetters />} />
                <Route path="/design" element={<TokenPalette />} />
                <Route path="*" element={<NotFound />} />
              </Routes>
            </Suspense>
          </Shell>
        </BrowserRouter>
      </QueryClientProvider>
    </ThemeProvider>
  )
}
