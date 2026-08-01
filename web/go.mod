// This module contains no Go source.
//
// It exists purely as a boundary marker: the Go toolchain does not descend
// into a nested module, so this file keeps `go build ./...`, `go test ./...`,
// `go vet ./...` and golangci-lint out of web/node_modules — where npm
// packages occasionally ship stray .go files (flatted/golang, for one) that
// would otherwise be matched, linted and vetted as if they were ours.
//
// The alternative is spelling out `./cmd/... ./internal/...` in every command
// and CI step, and forgetting it exactly once.

module github.com/SharmaSanskar/ithaca/web

go 1.25
