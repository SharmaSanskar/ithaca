// Empty the Vite output directory while preserving .gitkeep.
//
// Vite's own `emptyOutDir` would delete it, and that file is what lets
// `go build -tags embedweb ./...` compile on a checkout where npm has never
// run — without it the //go:embed directive fails with "no matching files".

import { readdirSync, rmSync, mkdirSync, writeFileSync, existsSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const dir = path.dirname(fileURLToPath(import.meta.url))
const dist = path.resolve(dir, '../../internal/web/dist')
const keep = new Set(['.gitkeep'])

if (!existsSync(dist)) {
  mkdirSync(dist, { recursive: true })
}

for (const entry of readdirSync(dist)) {
  if (keep.has(entry)) continue
  rmSync(path.join(dist, entry), { recursive: true, force: true })
}

const gitkeep = path.join(dist, '.gitkeep')
if (!existsSync(gitkeep)) {
  writeFileSync(
    gitkeep,
    '# Placeholder so `go build -tags embedweb` compiles on a clean checkout.\n' +
      '# The real contents are written by `npm run build` (vite outDir).\n',
  )
}
