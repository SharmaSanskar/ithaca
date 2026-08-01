import path from 'node:path'
import { fileURLToPath } from 'node:url'

import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const dir = path.dirname(fileURLToPath(import.meta.url))

export default defineConfig({
  plugins: [react(), tailwindcss()],

  resolve: {
    alias: { '@': path.resolve(dir, 'src') },
  },

  build: {
    // The Go binary embeds this directory (see internal/web/embed_on.go).
    // Building straight into it means `make build-full` is two commands with
    // no copy step to forget.
    outDir: path.resolve(dir, '../internal/web/dist'),

    // Cleaning is done by scripts/clean-dist.mjs instead, because Vite's
    // emptyOutDir would also delete the committed .gitkeep that lets
    // `go build -tags embedweb` compile on a fresh checkout.
    emptyOutDir: false,

    // Source maps are ~1MB and are not served to users by the Go handler
    // unless requested, but they make a production stack trace readable.
    sourcemap: true,

    rollupOptions: {
      output: {
        // Keep the heavy, rarely-changing libraries in their own chunk so a
        // dashboard code change does not invalidate a 200KB vendor bundle in
        // every browser cache.
        manualChunks: {
          react: ['react', 'react-dom', 'react-router'],
        },
      },
    },
  },

  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      // Proxying keeps the browser on a single origin in development, which
      // is what makes the production setup (SPA embedded in the Go binary,
      // same-origin cookies, no CORS) identical to the dev setup.
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: false,
      },
      '/ws': {
        target: 'ws://localhost:8082',
        ws: true,
      },
    },
  },
})
