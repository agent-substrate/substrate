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
	"log"
	"log/slog"
	"maps"
	"testing"
)

// TestResolveExporters pins the rules every OTEL_*_EXPORTER variable shares.
func TestResolveExporters(t *testing.T) {
	t.Parallel()

	known := []string{"otlp", "console"}
	otlp := Exporters{"otlp": true}
	both := Exporters{"otlp": true, "console": true}
	tests := []struct {
		name    string
		value   string
		def     Exporters
		want    Exporters
		wantErr bool
	}{
		{name: "unset or empty keeps the default", value: "", def: otlp, want: otlp},
		{name: "whitespace only keeps the default", value: "   ", def: otlp, want: otlp},
		{name: "one name", value: "otlp", def: Exporters{}, want: otlp},
		{name: "case and spaces do not matter", value: "  OTLP\t", def: Exporters{}, want: otlp},
		{name: "a list", value: " Console , OTLP ", def: Exporters{}, want: both},
		{name: "a repeated name counts once", value: "otlp,otlp", def: Exporters{}, want: otlp},
		{name: "an empty item is skipped with a warning", value: "otlp,", def: Exporters{}, want: otlp, wantErr: true},
		{name: "none alone is the empty set", value: "none", def: otlp, want: Exporters{}},
		{name: "an unknown item is skipped and the rest apply", value: "otlp,zipkin", def: Exporters{}, want: otlp, wantErr: true},
		{name: "unknown items next to none leave none", value: "none,zipkin", def: otlp, want: Exporters{}, wantErr: true},
		{name: "none with an exporter keeps the default", value: "none,otlp", def: Exporters{"console": true}, want: Exporters{"console": true}, wantErr: true},
		{name: "none after an exporter keeps the default", value: "otlp,none", def: Exporters{"console": true}, want: Exporters{"console": true}, wantErr: true},
		{name: "no known item keeps the default", value: "zipkin", def: otlp, want: otlp, wantErr: true},
		{name: "only empty items keep the default", value: ",", def: otlp, want: otlp, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveExporters("OTEL_TEST_EXPORTER", tt.value, known, tt.def)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveExporters(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
			// maps.Equal treats nil and empty alike, but nil means not chosen.
			if !maps.Equal(got, tt.want) || (got == nil) != (tt.want == nil) {
				t.Errorf("resolveExporters(%q) = %#v, want %#v", tt.value, got, tt.want)
			}
		})
	}
}

// TestResolveExportersEnvWarns pins that a problem with the value is logged,
// naming the set that is used, and that a valid or empty value is not.
func TestResolveExportersEnvWarns(t *testing.T) {
	const env = "OTEL_TEST_EXPORTER"
	known := []string{"otlp", "console"}
	// slog.SetDefault also points the log package at the handler, and setting
	// slog back does not undo that, so restore all three.
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	for value, wantUsing := range map[string]string{
		"otlp,zipkin": "otlp",
		"none,otlp":   "console",
		"":            "",
		"otlp":        "",
	} {
		rec := &warnRecorder{}
		slog.SetDefault(slog.New(rec))
		t.Setenv(env, value)
		resolveExportersEnv(context.Background(), env, known, Exporters{"console": true})

		switch {
		case wantUsing == "" && len(rec.using) != 0:
			t.Errorf("%q: got warnings %v, want none", value, rec.using)
		case wantUsing != "" && (len(rec.using) != 1 || rec.using[0] != wantUsing):
			t.Errorf("%q: got warnings using %v, want one using %q", value, rec.using, wantUsing)
		}
	}
}

// warnRecorder keeps the using attribute of each warning it is handed.
type warnRecorder struct{ using []string }

func (r *warnRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *warnRecorder) Handle(_ context.Context, rec slog.Record) error {
	if rec.Level != slog.LevelWarn {
		return nil
	}
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "using" {
			r.using = append(r.using, a.Value.String())
		}
		return true
	})
	return nil
}
func (r *warnRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *warnRecorder) WithGroup(string) slog.Handler      { return r }

// TestResolveExportersCopiesDefault pins that the result never aliases def, so
// a caller that changes its result cannot change a shared default.
func TestResolveExportersCopiesDefault(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "zipkin", "none,otlp"} {
		def := Exporters{"otlp": true}
		got, _ := resolveExporters("OTEL_TEST_EXPORTER", value, []string{"otlp"}, def)
		delete(got, "otlp")
		if !def.Has("otlp") {
			t.Errorf("resolveExporters(%q): changing the result changed def", value)
		}
	}
}

func TestExportersString(t *testing.T) {
	t.Parallel()
	for set, want := range map[string]Exporters{
		"none":         {},
		"otlp":         {"otlp": true},
		"console,otlp": {"otlp": true, "console": true},
		"a,b,c,d":      {"d": true, "c": true, "b": true, "a": true},
	} {
		// Map order is random, so repeat: one unsorted result fails the test.
		for range 50 {
			if got := want.String(); got != set {
				t.Fatalf("%v.String() = %q, want %q", want, got, set)
			}
		}
	}
}
