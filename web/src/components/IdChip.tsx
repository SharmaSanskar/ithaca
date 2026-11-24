import { Check, Copy } from 'lucide-react'
import { useState } from 'react'
import { Link } from 'react-router'

import { cn } from '@/lib/utils'

interface IdChipProps {
  id: string
  /** characters kept at the start */
  head?: number
  /** characters kept at the end */
  tail?: number
  href?: string
  className?: string
}

/**
 * A copyable identifier with a MIDDLE ellipsis: 01a2…e9f3. Never truncate the
 * tail: when you match an ID against a log line, the end is the part you
 * compare. Click copies; with href, the text is a link and the icon copies.
 */
export function IdChip({ id, head = 8, tail = 4, href, className }: IdChipProps) {
  const [copied, setCopied] = useState(false)
  const short = id.length > head + tail + 1 ? `${id.slice(0, head)}…${id.slice(-tail)}` : id

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(id)
      setCopied(true)
      setTimeout(() => setCopied(false), 1200)
    } catch {
      /* clipboard unavailable (insecure context); the full id is in the title */
    }
  }

  const text = (
    <span className="underline decoration-line-strong decoration-dotted underline-offset-2">
      {short}
    </span>
  )
  return (
    <span
      className={cn('group inline-flex items-center gap-1 t-xs font-mono text-fg-2', className)}
      title={id}
    >
      {href ? (
        <Link to={href} className="hover:text-fg">
          {text}
        </Link>
      ) : (
        text
      )}
      <button
        type="button"
        onClick={copy}
        aria-label={`Copy ${id}`}
        className="text-fg-3 opacity-0 transition-opacity group-hover:opacity-100 focus-visible:opacity-100 hover:text-fg-2"
      >
        {copied ? <Check size={11} /> : <Copy size={11} />}
      </button>
    </span>
  )
}
