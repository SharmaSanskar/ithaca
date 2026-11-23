// Package migrations embeds the goose SQL migrations into the binary, so
// `ithaca migrate` needs nothing on disk. In Kubernetes this runs as a Job (or
// init container) from the same image as the services — schema and code can
// never come from different commits.
package migrations

import "embed"

// FS holds every *.sql migration, in goose's numbered order.
//
//go:embed *.sql
var FS embed.FS
