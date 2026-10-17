// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

// Every setting must be reachable from all three channels. This is the whole
// point of the registry: a setting that reaches one channel and misses another
// is the defect it exists to prevent.
func TestRegistryCoversEveryChannel(t *testing.T) {
	for _, s := range Registry {
		t.Run(s.Key, func(t *testing.T) {
			if s.Key == "" {
				t.Error("Key is empty")
			}
			if s.Env == "" {
				t.Error("Env is empty; the setting cannot be set from the environment")
			}
			// Secrets are the exception: a command line is world-readable
			// in `ps` and is kept in shell history, so a credential is
			// supplied through the environment or a file instead.
			if s.Flag == "" && !s.Secret {
				t.Error("Flag is empty; the setting cannot be set from the command line")
			}
			if s.Usage == "" {
				t.Error("Usage is empty; the flag would have no help text")
			}
		})
	}
}

func TestRegistryNamesAreUnique(t *testing.T) {
	for _, field := range []struct {
		name string
		get  func(Setting) string
	}{
		{"Key", func(s Setting) string { return s.Key }},
		{"Env", func(s Setting) string { return s.Env }},
		{"Flag", func(s Setting) string { return s.Flag }},
	} {
		t.Run(field.name, func(t *testing.T) {
			seen := map[string]string{}
			for _, s := range Registry {
				v := field.get(s)
				// Several secrets declare no flag; that is not a collision.
				if v == "" {
					continue
				}
				if prev, dup := seen[v]; dup {
					t.Errorf("%s %q used by both %s and %s", field.name, v, prev, s.Key)
				}
				seen[v] = s.Key
			}
		})
	}
}

// A default must itself be valid, or the install fails before the operator has
// done anything.
func TestRegistryDefaultsParse(t *testing.T) {
	for _, s := range Registry {
		t.Run(s.Key, func(t *testing.T) {
			if s.Default == "" {
				return
			}
			if _, err := s.parse(s.Default); err != nil {
				t.Errorf("default %q does not parse: %v", s.Default, err)
			}
		})
	}
}

func TestLookup(t *testing.T) {
	if _, ok := Lookup("atenet.dataplane"); !ok {
		t.Error("Lookup(atenet.dataplane) = false, want true for a declared setting")
	}
	if _, ok := Lookup("no.such.setting"); ok {
		t.Error("Lookup(no.such.setting) = true, want false")
	}
}

func TestBindFlagsRegistersEverySetting(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(fs)
	for _, s := range Registry {
		if s.Flag == "" {
			continue
		}
		if fs.Lookup(s.Flag) == nil {
			t.Errorf("flag --%s was not registered for %s", s.Flag, s.Key)
		}
	}
}

// A flag registers with its declared default so --help shows what an operator
// gets without it. Registering it is safe because pflag sets Changed only from
// Set: an untouched flag still reads as unsupplied, which is what Resolve keys
// precedence off.
func TestBindFlagsRegistersDeclaredDefaults(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(fs)
	for _, s := range Registry {
		if s.Default == "" {
			continue
		}
		t.Run(s.Key, func(t *testing.T) {
			f := fs.Lookup(s.Flag)
			if f.DefValue != s.Default {
				t.Errorf("flag --%s registered with default %q, want %q", s.Flag, f.DefValue, s.Default)
			}
			if fs.Changed(s.Flag) {
				t.Errorf("flag --%s reports Changed before anything set it", s.Flag)
			}
		})
	}
}

// The default has to reach the help text, which is the whole reason for
// registering it.
func TestDeclaredDefaultsAppearInHelp(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(fs)
	help := fs.FlagUsages()
	// pflag suppresses the default when it equals the zero value of the flag's
	// type, so a setting declaring that zero has nothing to show.
	zero := map[ValueKind]string{KindBool: "false", KindInt: "0", KindDuration: "0s"}
	for _, s := range Registry {
		if s.Default == "" || s.Default == zero[s.Kind] {
			continue
		}
		t.Run(s.Key, func(t *testing.T) {
			if !strings.Contains(help, "(default "+s.Default+")") &&
				!strings.Contains(help, `(default "`+s.Default+`")`) {
				t.Errorf("--%s does not show its default %q in help", s.Flag, s.Default)
			}
		})
	}
}

// The three channels have to agree on what an integer is. A value one accepts
// and another rejects means a record written from one run cannot be replayed
// through a different channel.
func TestIntAcceptanceIsTheSameOnEveryChannel(t *testing.T) {
	useSettings(t, Setting{
		Key: "scratch.count", Env: "SCRATCH_COUNT", Flag: "scratch-count",
		Kind: KindInt, Default: "1", Usage: "an int",
	})

	// Rejection is compared through Resolve rather than at the flag set,
	// because that is where a value becomes a setting on every channel.
	resolveFlag := func(t *testing.T, raw string) error {
		t.Helper()
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		BindFlags(fs)
		if err := fs.Set("scratch-count", raw); err != nil {
			return err
		}
		_, err := Resolve(fs, ResolveOptions{})
		return err
	}

	for _, raw := range []string{"3x", "2.5", "0x10", "1e3", "12 34"} {
		t.Run(raw, func(t *testing.T) {
			_, envErr := Resolve(nil, ResolveOptions{Env: map[string]string{"SCRATCH_COUNT": raw}})
			_, fileErr := Resolve(nil, ResolveOptions{File: fileWith(map[string]string{"scratch.count": raw})})
			flagErr := resolveFlag(t, raw)

			for _, c := range []struct {
				channel string
				err     error
			}{{"environment", envErr}, {"file", fileErr}, {"flag", flagErr}} {
				if c.err == nil {
					t.Errorf("the %s channel accepted %q as an integer", c.channel, raw)
				}
			}
		})
	}
}

func TestSettingParse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    ValueKind
		raw     string
		want    any
		wantErr bool
	}{
		{name: "string passes through", kind: KindString, raw: "envoy", want: "envoy"},
		{name: "empty string", kind: KindString, raw: "", want: ""},
		{name: "bool true", kind: KindBool, raw: "true", want: true},
		{name: "bool 1", kind: KindBool, raw: "1", want: true},
		{name: "bool false", kind: KindBool, raw: "false", want: false},
		{name: "bool 0", kind: KindBool, raw: "0", want: false},
		{name: "bool empty is false", kind: KindBool, raw: "", want: false},
		{name: "bool rejects other", kind: KindBool, raw: "yes", wantErr: true},
		{name: "int", kind: KindInt, raw: "4", want: 4},
		{name: "int rejects words", kind: KindInt, raw: "four", wantErr: true},

		// An integer is the whole value or nothing. Accepting a prefix and
		// discarding the rest would let a typo through as a plausible number,
		// and pflag rejects all of these on the flag channel.
		{name: "int rejects a trailing suffix", kind: KindInt, raw: "3x", wantErr: true},
		{name: "int rejects a decimal", kind: KindInt, raw: "2.5", wantErr: true},
		{name: "int rejects hex", kind: KindInt, raw: "0x10", wantErr: true},
		{name: "int rejects exponent notation", kind: KindInt, raw: "1e3", wantErr: true},
		{name: "int rejects a second number", kind: KindInt, raw: "12 34", wantErr: true},
		{name: "duration", kind: KindDuration, raw: "90s", want: 90 * time.Second},
		{name: "duration rejects bare number", kind: KindDuration, raw: "90", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := Setting{Key: "test.key", Kind: tc.kind}
			got, err := s.parse(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parse(%q) = %v, want error", tc.raw, got)
				}
				// The message names the setting, not the Go type, so the
				// reader knows what to fix.
				if !strings.Contains(err.Error(), "test.key") {
					t.Errorf("error %q does not name the setting", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse(%q) error = %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("parse(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestKeysAreSorted(t *testing.T) {
	keys := Keys()
	if len(keys) != len(Registry) {
		t.Fatalf("Keys() returned %d entries, want %d", len(keys), len(Registry))
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1] > keys[i] {
			t.Errorf("Keys() not sorted: %q before %q", keys[i-1], keys[i])
		}
	}
}

// A setting key must not collide with the document's own header fields. A
// top-level "kind" setting would overwrite kind: SubstrateInstall and make
// every document fail its header check.
func TestRegistryKeysDoNotCollideWithDocumentHeader(t *testing.T) {
	for _, reserved := range []string{"apiVersion", "kind", documentMetadataKey} {
		for _, s := range Registry {
			root, _, _ := strings.Cut(s.Key, ".")
			if root == reserved {
				t.Errorf("setting %q uses reserved document field %q as its root", s.Key, reserved)
			}
		}
	}
}

// A secret must not be settable on the command line: a flag puts the value in
// shell history and in the process list, where the environment and the file
// do not. These three were environment-only before the registry gave every
// setting a flag.
func TestSecretSettingsHaveNoFlag(t *testing.T) {
	for _, s := range All() {
		if !s.Secret {
			continue
		}
		t.Run(s.Key, func(t *testing.T) {
			if s.Flag != "" {
				t.Errorf("--%s exposes a credential on the command line", s.Flag)
			}
			if s.Env == "" {
				t.Errorf("%s has no environment variable, so it cannot be set at all", s.Key)
			}
		})
	}
}
