// Command ithaca is the single binary for every Ithaca service.
//
// One module, one image, one entrypoint, a subcommand per service:
//
//	ithaca ingest        accept events (public write path)
//	ithaca dispatcher    lease streams and deliver (the worker pool)
//	ithaca api           control plane + embedded dashboard
//	ithaca ws            WebSocket hub for the live feed
//	ithaca worker        periodic work: fanout, reaper, partitions, replay
//	ithaca receiver-sim  programmable subscriber, for demos and load tests
//
// Shipping one image instead of five halves the CI matrix and makes it
// impossible to deploy a dispatcher built from a different commit than the
// ingest gateway it shares a database schema with.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
	"github.com/SharmaSanskar/ithaca/internal/platform/shutdown"
	"github.com/SharmaSanskar/ithaca/internal/platform/version"
)

func main() {
	if err := run(os.Args); err != nil {
		// Config and bind failures happen before the logger exists, so this
		// deliberately goes to stderr in plain text rather than as JSON.
		fmt.Fprintf(os.Stderr, "ithaca: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		usage(os.Stderr)
		return errors.New("no subcommand given")
	}

	switch cmd := args[1]; cmd {
	case "-h", "--help", "help":
		usage(os.Stdout)
		return nil
	case "-v", "--version", "version":
		fmt.Println(version.String())
		return nil
	default:
		svc, ok := config.ParseService(cmd)
		if !ok {
			usage(os.Stderr)
			return fmt.Errorf("unknown subcommand %q", cmd)
		}
		return serve(svc)
	}
}

func serve(svc config.Service) error {
	cfg, err := config.Load(svc)
	if err != nil {
		return err
	}

	app := newApp(cfg)
	if err := app.wire(); err != nil {
		return err
	}

	ctx, stop := shutdown.Signals(context.Background())
	defer stop()

	return app.run(ctx)
}

func usage(w *os.File) {
	names := make([]string, 0, len(config.Services))
	for _, s := range config.Services {
		names = append(names, string(s))
	}
	fmt.Fprintf(w, `%s

usage: ithaca <service>

services:
  %s

other:
  version    print build information
  help       print this message

Configuration comes from the environment; see .env.example.
`, version.String(), strings.Join(names, "\n  "))
}
