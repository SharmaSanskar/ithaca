//go:build embedweb

package web

import (
	"embed"
	"io/fs"
)

// dist holds the Vite build output. `all:` is required so Vite's dotfiles
// (and the .gitkeep that makes this compile on a clean checkout) are included.
//
//go:embed all:dist
var distFS embed.FS

func dist() (fs.FS, bool) {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil, false
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		// Tag was set but `npm run build` never ran. Fall back to the
		// placeholder rather than serving a 404 for every route.
		return nil, false
	}
	return sub, true
}
