// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverboot

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
)

// exporterNone selects the empty set; it is not an exporter.
const exporterNone = "none"

// The exporter names the OTEL_*_EXPORTER variables use, for checking a
// resolved set with Has.
const (
	ExporterOTLP    = "otlp"
	ExporterConsole = "console"
)

// Exporters is the set of exporters an OTEL_*_EXPORTER variable selects. The
// empty set is none; nil means not chosen.
type Exporters map[string]bool

// Has reports whether name is selected.
func (e Exporters) Has(name string) bool { return e[name] }

// String lists the names in order, or none.
func (e Exporters) String() string {
	if len(e) == 0 {
		return exporterNone
	}
	return strings.Join(slices.Sorted(maps.Keys(e)), ",")
}

// resolveExportersEnv reads env and applies resolveExporters, logging any
// problem with the value rather than failing startup over a telemetry setting.
func resolveExportersEnv(ctx context.Context, env string, known []string, def Exporters) Exporters {
	value := os.Getenv(env)
	got, err := resolveExporters(env, value, known, def)
	if err != nil {
		slog.WarnContext(ctx, "Invalid exporter environment",
			slog.String("env", env),
			slog.String("value", value),
			slog.String("using", got.String()),
			slog.Any("err", err))
	}
	return got
}

// resolveExporters applies the rules every OTEL_*_EXPORTER variable shares to
// a comma-separated list, as the spec allows. Names are trimmed, lowercased,
// and counted once; empty and unknown items are skipped, and the rest apply.
// none with an exporter is rejected, and a value that is empty, rejected, or
// names no known exporter gives a copy of def. A non-nil error
// says what to warn about; the returned set is the one to use either way.
func resolveExporters(env, value string, known []string, def Exporters) (Exporters, error) {
	// Unset and empty read the same: templated manifests can render empty env vars.
	if strings.TrimSpace(value) == "" {
		return maps.Clone(def), nil
	}

	got := Exporters{}
	var none bool
	var skipped []string
	for item := range strings.SplitSeq(value, ",") {
		switch name := strings.ToLower(strings.TrimSpace(item)); {
		case name == exporterNone:
			none = true
		case slices.Contains(known, name):
			got[name] = true
		default:
			skipped = append(skipped, name)
		}
	}

	var skipErr error
	if len(skipped) > 0 {
		skipErr = fmt.Errorf("%s: skipped empty or unknown exporters %q", env, skipped)
	}
	switch {
	case none && len(got) > 0:
		return maps.Clone(def), fmt.Errorf("%s %q lists none with an exporter", env, value)
	case none:
		return Exporters{}, skipErr
	case len(got) == 0:
		return maps.Clone(def), fmt.Errorf("%s %q names no known exporter", env, value)
	}
	return got, skipErr
}
