import { RotateCcw } from 'lucide-react'
import { useState } from 'react'
import { Link } from 'react-router'

import { IdChip } from '@/components/IdChip'
import { StatusPill } from '@/components/StatusPill'
import { Table, Td, Th } from '@/components/Table'
import { Button, Empty, Panel, Skeleton } from '@/components/ui'
import { useDeadLetters, useOverview, useReplay } from '@/lib/api'
import { ago } from '@/lib/time'

/**
 * The dead-letter queue. A dead letter is kept, not lost: replaying it creates
 * a NEW delivery for the SAME event (same Ithaca-Id), so a subscriber that
 * already processed it can ignore the duplicate. Replay is idempotent: a
 * double-click replays nothing twice.
 */
export default function DeadLetters() {
  const [endpoint, setEndpoint] = useState<string>('')
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const { data: ov } = useOverview()
  const names = new Map((ov?.endpoints ?? []).map((e) => [e.id, e.name]))
  const q = useDeadLetters(endpoint || undefined)
  const replay = useReplay()
  const rows = q.data?.pages.flatMap((p) => p.data) ?? []
  const open = rows.filter((r) => !r.replayed_at)

  const toggle = (id: string) =>
    setSelected((s) => {
      const n = new Set(s)
      if (n.has(id)) n.delete(id)
      else n.add(id)
      return n
    })
  const result = replay.data

  return (
    <Panel
      title="Dead-letter queue"
      actions={
        <>
          <select
            value={endpoint}
            onChange={(e) => {
              setEndpoint(e.target.value)
              setSelected(new Set())
            }}
            className="rounded-[4px] border border-line-strong bg-surface px-1.5 py-0.5 t-xs text-fg"
            aria-label="Filter by endpoint"
          >
            <option value="">All endpoints</option>
            {(ov?.endpoints ?? []).map((e) => (
              <option key={e.id} value={e.id}>
                {e.name}
              </option>
            ))}
          </select>
          <Button
            size="xs"
            disabled={selected.size === 0 || replay.isPending}
            onClick={() =>
              replay.mutate({ ids: [...selected] }, { onSuccess: () => setSelected(new Set()) })
            }
          >
            <RotateCcw size={11} /> Replay {selected.size || ''} selected
          </Button>
          <Button
            size="xs"
            variant="primary"
            disabled={open.length === 0 || replay.isPending}
            onClick={() => replay.mutate(endpoint ? { endpoint_id: endpoint } : { all: true })}
          >
            Replay all
          </Button>
        </>
      }
    >
      {result && (
        <p className="border-b border-line bg-st-delivered/8 px-3 py-1.5 t-xs text-st-delivered-ink">
          Replayed {result.replayed}
          {result.resumed_streams > 0 &&
            `, resuming ${result.resumed_streams} paused ordered stream${result.resumed_streams > 1 ? 's' : ''} in original order`}
          . Same event ids, new delivery ids, ramped per endpoint.
        </p>
      )}
      {q.isPending ? (
        <div className="flex flex-col gap-2 p-3">
          {Array.from({ length: 6 }, (_, i) => (
            <Skeleton key={i} className="h-6" />
          ))}
        </div>
      ) : rows.length === 0 ? (
        <Empty>
          The dead-letter queue is empty. Run “Bad deploy” in the demo console to fill it.
        </Empty>
      ) : (
        <>
          <Table
            head={
              <>
                <Th className="w-8">
                  <input
                    type="checkbox"
                    aria-label="Select all"
                    checked={open.length > 0 && open.every((r) => selected.has(r.id))}
                    onChange={(e) =>
                      setSelected(e.target.checked ? new Set(open.map((r) => r.id)) : new Set())
                    }
                  />
                </Th>
                <Th>State</Th>
                <Th>Event</Th>
                <Th>Endpoint</Th>
                <Th>Reason</Th>
                <Th className="text-right">Attempts</Th>
                <Th className="text-right">Died</Th>
              </>
            }
          >
            {rows.map((r) => (
              <tr
                key={r.id}
                className="hover:bg-hover"
                data-status={r.replayed_at ? 'replayed' : 'dead'}
              >
                <Td>
                  <input
                    type="checkbox"
                    aria-label={`Select ${r.event_type} ${r.id}`}
                    disabled={!!r.replayed_at}
                    checked={selected.has(r.id)}
                    onChange={() => toggle(r.id)}
                  />
                </Td>
                <Td>
                  {r.replayed_at ? (
                    <span className="t-micro font-mono text-st-delivered-ink">replayed</span>
                  ) : (
                    <span className="flex items-center gap-2">
                      <StatusPill status="dead" />
                      {r.blocks_stream && (
                        <span
                          className="t-micro font-mono text-st-dead-ink"
                          title="An ordered stream is paused behind this delivery. Replaying it resumes the stream in original order."
                        >
                          blocks stream
                        </span>
                      )}
                    </span>
                  )}
                </Td>
                <Td>
                  <Link to={`/events/${r.event_id}`} className="text-fg hover:underline">
                    {r.event_type}
                  </Link>{' '}
                  <IdChip id={r.event_id} />
                </Td>
                <Td className="t-xs text-fg-2">{names.get(r.endpoint_id) ?? '—'}</Td>
                <Td
                  className="max-w-[260px] truncate font-mono t-xs text-fg-3"
                  title={r.last_error ?? ''}
                >
                  {r.dead_reason}
                </Td>
                <Td className="text-right font-mono t-xs">{r.attempts}</Td>
                <Td className="text-right t-xs text-fg-3">{ago(r.died_at)}</Td>
              </tr>
            ))}
          </Table>
          {q.hasNextPage && (
            <div className="flex justify-center border-t border-line p-2">
              <Button
                size="xs"
                onClick={() => void q.fetchNextPage()}
                disabled={q.isFetchingNextPage}
              >
                Load older
              </Button>
            </div>
          )}
        </>
      )}
    </Panel>
  )
}
