//go:build !embedweb

package web

import "io/fs"

// dist reports no assets: this binary was built without the `embedweb` tag, so
// `go build ./...` works on a checkout where npm has never run.
func dist() (fs.FS, bool) { return nil, false }
