import type { ButtonHTMLAttributes, ReactNode, Ref } from 'react'

import { cn } from '@/lib/utils'

/** Level-1 surface: bg-surface + 1px hairline. No shadow, ever. */
export function Panel({
  title,
  actions,
  children,
  className,
  bodyClassName,
}: {
  title?: ReactNode
  actions?: ReactNode
  children: ReactNode
  className?: string
  bodyClassName?: string
}) {
  return (
    <section className={cn('panel flex min-h-0 flex-col', className)}>
      {(title || actions) && (
        <header className="flex h-9 shrink-0 items-center justify-between gap-2 border-b border-line px-3">
          <h2 className="t-micro font-mono text-fg-3">{title}</h2>
          <div className="flex items-center gap-2">{actions}</div>
        </header>
      )}
      <div className={cn('min-h-0 flex-1', bodyClassName)}>{children}</div>
    </section>
  )
}

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger'

export function Button({
  variant = 'secondary',
  size = 'sm',
  className,
  ref,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: Variant
  size?: 'xs' | 'sm'
  ref?: Ref<HTMLButtonElement>
}) {
  return (
    <button
      ref={ref}
      type="button"
      className={cn(
        'inline-flex items-center justify-center gap-1.5 rounded-[5px] border font-medium whitespace-nowrap transition-colors disabled:pointer-events-none disabled:opacity-50',
        size === 'xs' ? 'h-6 px-2 t-xs' : 'h-7 px-2.5 t-sm',
        variant === 'primary' && 'border-transparent bg-signal text-canvas hover:opacity-90',
        variant === 'secondary' && 'border-line-strong bg-surface text-fg hover:bg-hover',
        variant === 'ghost' && 'border-transparent text-fg-2 hover:bg-hover hover:text-fg',
        variant === 'danger' && 'border-line-strong bg-surface text-st-failed-ink hover:bg-hover',
        className,
      )}
      {...props}
    />
  )
}

/** A label/value pair in the dimmest-to-brightest hierarchy. */
export function Stat({
  label,
  children,
  className,
}: {
  label: string
  children: ReactNode
  className?: string
}) {
  return (
    <div className={cn('flex flex-col gap-0.5', className)}>
      <span className="t-micro font-mono text-fg-3">{label}</span>
      <span className="t-sm text-fg">{children}</span>
    </div>
  )
}

export function Empty({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-full min-h-24 items-center justify-center p-6 text-center t-sm text-fg-3">
      {children}
    </div>
  )
}

/** A shimmer placeholder: opacity ≤ 6%, never louder than the content. */
export function Skeleton({ className }: { className?: string }) {
  return <div className={cn('animate-pulse rounded-[4px] bg-fg/5', className)} />
}
