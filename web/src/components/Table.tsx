import type { ReactNode } from 'react'

import { cn } from '@/lib/utils'

/** A dense, hairline table. Rows use the density token for height. */
export function Table({
  head,
  children,
  className,
}: {
  head: ReactNode
  children: ReactNode
  className?: string
}) {
  return (
    <div className={cn('overflow-x-auto', className)}>
      <table className="w-full border-collapse t-sm">
        <thead className="sticky top-0 z-10 bg-inset">
          <tr className="text-left">{head}</tr>
        </thead>
        <tbody className="divide-y divide-line-faint">{children}</tbody>
      </table>
    </div>
  )
}

export function Th({ children, className }: { children?: ReactNode; className?: string }) {
  return (
    <th
      className={cn(
        'h-8 border-b border-line px-3 t-micro font-mono font-normal whitespace-nowrap text-fg-3',
        className,
      )}
    >
      {children}
    </th>
  )
}

export function Td({
  children,
  className,
  title,
}: {
  children?: ReactNode
  className?: string
  title?: string
}) {
  return (
    <td title={title} className={cn('h-[var(--row-h)] px-3 whitespace-nowrap', className)}>
      {children}
    </td>
  )
}
