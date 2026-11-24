import { Link } from 'react-router'

export default function NotFound() {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-2 p-12 text-center">
      <p className="t-micro font-mono text-fg-3">404</p>
      <p className="t-md text-fg">Nothing is delivered to this address.</p>
      <Link to="/" className="t-sm text-signal-ink hover:underline">
        Back to mission control
      </Link>
    </div>
  )
}
