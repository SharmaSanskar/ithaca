import { useSyncExternalStore } from 'react'

const query =
  typeof window !== 'undefined' ? window.matchMedia('(prefers-reduced-motion: reduce)') : null

/** One global switch: reduced motion removes movement and keeps the numbers. */
export function prefersReducedMotion(): boolean {
  return query?.matches ?? false
}

export function useReducedMotion(): boolean {
  return useSyncExternalStore((fn) => {
    query?.addEventListener('change', fn)
    return () => query?.removeEventListener('change', fn)
  }, prefersReducedMotion)
}
