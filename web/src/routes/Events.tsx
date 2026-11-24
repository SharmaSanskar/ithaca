import { Link } from 'react-router'

import { IdChip } from '@/components/IdChip'
import { StatusPill } from '@/components/StatusPill'
import { Table, Td, Th } from '@/components/Table'
import { Button, Empty, Panel, Skeleton } from '@/components/ui'
import { useEvents } from '@/lib/api'
import { ago } from '@/lib/time'

/** Recent events, newest first, keyset-paginated (stable while new events arrive). */
export default function Events() {
  const q = useEvents()
  const rows = q.data?.pages.flatMap((p) => p.data) ?? []

  return (
    <Panel
      title="Events"
      actions={<span className="t-xs text-fg-3">auto-refreshing every 5s</span>}
    >
      {q.isPending ? (
        <div className="flex flex-col gap-2 p-3">
          {Array.from({ length: 8 }, (_, i) => (
            <Skeleton key={i} className="h-6" />
          ))}
        </div>
      ) : rows.length === 0 ? (
        <Empty>No events yet. Start traffic from the demo console.</Empty>
      ) : (
        <>
          <Table
            head={
              <>
                <Th>Status</Th>
                <Th>Type</Th>
                <Th>Ordering key</Th>
                <Th>Deliveries</Th>
                <Th>Event</Th>
                <Th className="text-right">Received</Th>
              </>
            }
          >
            {rows.map((e) => (
              <tr key={e.id} className="hover:bg-hover">
                <Td>
                  <StatusPill status={e.dead ? 'dead' : e.pending ? 'queued' : 'delivered'} />
                </Td>
                <Td>
                  <Link to={`/events/${e.id}`} className="text-fg hover:underline">
                    {e.type}
                  </Link>
                </Td>
                <Td className="font-mono t-xs text-fg-3">{e.ordering_key ?? '—'}</Td>
                <Td className="t-xs text-fg-3">
                  <span className="text-st-delivered-ink">{e.succeeded} delivered</span>
                  {e.pending > 0 && <span> · {e.pending} pending</span>}
                  {e.dead > 0 && <span className="text-st-dead-ink"> · {e.dead} dead</span>}
                </Td>
                <Td>
                  <IdChip id={e.id} href={`/events/${e.id}`} />
                </Td>
                <Td className="text-right t-xs text-fg-3">{ago(e.created_at)}</Td>
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
                {q.isFetchingNextPage ? 'Loading…' : 'Load older'}
              </Button>
            </div>
          )}
        </>
      )}
    </Panel>
  )
}
