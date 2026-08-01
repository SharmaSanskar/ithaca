/**
 * Delivery state, and the words the product uses for it.
 *
 * Kept out of the component file so that editing a label does not force a full
 * remount of every table row during development (React Fast Refresh only
 * preserves state for modules that export components exclusively).
 */

export type Status = 'queued' | 'delivering' | 'delivered' | 'retrying' | 'failed' | 'dead'

/** Canonical order: roughly the path an event takes, ending at the two exits. */
export const STATUSES: readonly Status[] = [
  'queued',
  'delivering',
  'delivered',
  'retrying',
  'failed',
  'dead',
] as const

export const STATUS_LABELS: Record<Status, string> = {
  queued: 'Queued',
  delivering: 'Delivering',
  delivered: 'Delivered',
  retrying: 'Retrying',
  failed: 'Failed',
  dead: 'Dead',
}

/**
 * Tooltip and screen-reader text.
 *
 * These are the definitions the product commits to. "Failed" is one attempt;
 * "dead" is the end of the road — conflating them is the most common confusion
 * a webhook dashboard creates, so the wording draws the line explicitly.
 */
export const STATUS_DESCRIPTIONS: Record<Status, string> = {
  queued: 'Accepted and waiting in the queue. No delivery attempt yet.',
  delivering: 'An HTTP request to the subscriber endpoint is in flight.',
  delivered: 'The subscriber returned 2xx. Terminal, successful.',
  retrying: 'The attempt failed retryably and is scheduled with backoff.',
  failed: 'This attempt failed. Further attempts may remain.',
  dead: 'All attempts exhausted. Moved to the dead-letter queue.',
}
