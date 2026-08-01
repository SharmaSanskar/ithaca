// Package version exposes build metadata. Values are stamped at link time via
// -ldflags "-X .../version.Version=v1.2.3"; anything not stamped falls back to
// the VCS info the Go toolchain embeds automatically.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
)

var (
	// Version is the semantic version or image tag.
	Version = "dev"
	// Commit is the git SHA.
	Commit = ""
	// Date is the RFC3339 build timestamp.
	Date = ""
)

var buildInfo = sync.OnceValue(func() map[string]string {
	out := map[string]string{}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return out
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			out["revision"] = s.Value
		case "vcs.time":
			out["time"] = s.Value
		case "vcs.modified":
			out["modified"] = s.Value
		}
	}
	return out
})

// Revision returns the git SHA, preferring the linker-stamped value.
func Revision() string {
	if Commit != "" {
		return Commit
	}
	if v := buildInfo()["revision"]; v != "" {
		if buildInfo()["modified"] == "true" {
			return v + "-dirty"
		}
		return v
	}
	return "unknown"
}

// BuildDate returns the build timestamp, preferring the linker-stamped value.
func BuildDate() string {
	if Date != "" {
		return Date
	}
	if v := buildInfo()["time"]; v != "" {
		return v
	}
	return "unknown"
}

// Info is the structured form, used for the /version endpoint and startup logs.
func Info() map[string]string {
	return map[string]string{
		"version":   Version,
		"revision":  Revision(),
		"buildDate": BuildDate(),
		"goVersion": runtime.Version(),
		"platform":  runtime.GOOS + "/" + runtime.GOARCH,
	}
}

// String is the one-line human form.
func String() string {
	return fmt.Sprintf("ithaca %s (%s, built %s, %s, %s/%s)",
		Version, Revision(), BuildDate(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
