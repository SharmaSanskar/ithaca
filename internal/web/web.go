// Package web serves the dashboard single-page app.
//
// The SPA is compiled into the Go binary with embed.FS under the `embedweb`
// build tag. That is the whole reason this project is a Vite SPA rather than
// Next.js: one binary, one image, one Deployment, one Ingress, and same-origin
// cookies and WebSockets with no CORS anywhere. It is how Grafana, Prometheus
// and Traefik ship their UIs.
//
// The build tag keeps `go build ./...` working without running npm first,
// which matters for a fast CI lint job and for anyone touching only Go.
package web

import (
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// Embedded reports whether this binary carries a built SPA.
func Embedded() bool {
	_, ok := dist()
	return ok
}

// Handler serves the SPA, or a helpful placeholder when the assets were not
// compiled in.
func Handler(devServerURL string) http.Handler {
	f, ok := dist()
	if !ok {
		return placeholder(devServerURL)
	}
	return spaHandler(f)
}

// spaHandler serves hashed assets aggressively cached and everything else
// falling back to index.html so client-side routes deep-link correctly.
func spaHandler(f fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(f))

	index, err := fs.ReadFile(f, "index.html")
	if err != nil {
		return placeholder("")
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		upath := path.Clean("/" + r.URL.Path)
		name := strings.TrimPrefix(upath, "/")

		if name != "" {
			if st, err := fs.Stat(f, name); err == nil && !st.IsDir() {
				// Vite emits content-hashed filenames under /assets, so those
				// are safe to cache forever. Everything else gets revalidated.
				if strings.HasPrefix(upath, "/assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				} else {
					w.Header().Set("Cache-Control", "public, max-age=3600")
				}
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		// Unknown path → the SPA shell. index.html must never be cached, or a
		// deploy leaves browsers holding a shell that references asset hashes
		// which no longer exist.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(index)
	})
}

func placeholder(devServerURL string) http.Handler {
	hint := "Run `make web-dev` (Vite on http://localhost:5173) for the dashboard."
	if devServerURL != "" {
		hint = fmt.Sprintf("The dashboard dev server should be running at %s.", devServerURL)
	}
	body := fmt.Sprintf(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Ithaca</title>
<style>
  :root { color-scheme: dark; }
  body { margin:0; min-height:100vh; display:grid; place-items:center;
         background:#090a0d; color:#f4f5f8;
         font: 13px/1.5 ui-monospace, SFMono-Regular, monospace; }
  main { max-width: 46ch; padding: 24px; border: 1px solid rgba(255,255,255,.09);
         border-radius: 6px; background:#111316; }
  h1 { margin:0 0 12px; font-size:16px; letter-spacing:-.01em; }
  p  { margin:0 0 8px; color:#b5b7bd; }
  code { color:#57d4d4; }
</style></head>
<body><main>
  <h1>ithaca</h1>
  <p>This binary was built without the dashboard assets.</p>
  <p>%s</p>
  <p>To embed them: <code>make web-build &amp;&amp; go build -tags embedweb ./cmd/ithaca</code></p>
</main></body></html>`, hint)

	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})
}
