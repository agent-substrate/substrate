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
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
)

// ValueKind is the type of a setting's value.
type ValueKind int

const (
	KindString ValueKind = iota
	KindBool
	KindInt
	KindDuration
)

// Setting declares one configurable value. It is the single definition: the
// flag set, the environment reader and the file schema are all projections of
// Registry, so a setting cannot reach one channel and miss another.
type Setting struct {
	// Key is the canonical name and the path in the configuration file,
	// dot-separated: "ateapi.postgres.connectionString".
	Key string
	// Env is the environment variable that supplies this setting.
	Env string
	// Flag is the command-line name, without the leading dashes.
	Flag string
	// Kind is the value type. Resolve parses raw strings against it.
	Kind ValueKind
	// Default applies when no channel supplies a value.
	Default string
	// Secret suppresses the value wherever configuration is displayed.
	Secret bool
	// Usage is the flag help text.
	Usage string
	// Commands restricts the flag to those command paths, named as they are
	// typed: "deploy benchmarks". Empty means a persistent flag on the root.
	//
	// Only the flag channel is scoped. The environment and the file reach
	// every setting, because a configuration document describes a whole
	// install and must parse the same way whichever subcommand reads it.
	Commands []string
}

// Registry is the settings the installer itself owns.
//
// TODO: empty until the settings are moved here. Each one currently lives in
// whichever place reads it -- a flag registration, an env lookup in the
// loader, a demo's own flag set -- so moving them is the wiring change, not
// this one. This package is the mechanism: the declaration, the layering, the
// file format, the errors, the report and the record.
//
// Settings that belong to one command are added by RegisterCommand instead;
// All returns both.
var Registry = []Setting{}

// scoped holds the settings commands registered for themselves, in
// registration order.
var scoped []Setting

// byKey indexes Registry and everything RegisterCommand has added.
var byKey = func() map[string]Setting {
	m := make(map[string]Setting, len(Registry))
	for _, s := range Registry {
		m[s.Key] = s
	}
	return m
}()

// RegisterCommand records settings owned by one command, to be called from the
// owning package's init. A command declares its own configuration rather than
// a central list growing a line for every command, which is how demos.Register
// already works.
//
// It does not validate. A panic here would fire during package initialization,
// before any test could observe it, and would abort the test binary instead of
// failing a test. Validate reports the same problems and is callable.
func RegisterCommand(settings ...Setting) {
	for _, s := range settings {
		scoped = append(scoped, s)
		byKey[s.Key] = s
	}
}

// All returns every setting, installer-owned and command-registered, in
// declaration order. The file and the environment read this, so a
// configuration document parses identically whichever subcommand runs.
func All() []Setting {
	out := make([]Setting, 0, len(Registry)+len(scoped))
	out = append(out, Registry...)
	return append(out, scoped...)
}

// Validate reports duplicate names and missing channels across the merged set.
//
// It cannot run inside package config's own tests: the settings it is meant to
// catch are registered by the command packages, which config does not import.
// The caller is the test in package main, which imports the whole command tree,
// and Execute, so a bad registration fails at startup.
func Validate() error {
	var problems []string
	for _, field := range []struct {
		name string
		get  func(Setting) string
	}{
		{"key", func(s Setting) string { return s.Key }},
		{"environment variable", func(s Setting) string { return s.Env }},
		{"flag", func(s Setting) string { return s.Flag }},
	} {
		seen := map[string]string{}
		for _, s := range All() {
			v := field.get(s)
			if v == "" {
				problems = append(problems,
					fmt.Sprintf("setting %q has no %s", s.Key, field.name))
				continue
			}
			if prev, dup := seen[v]; dup {
				problems = append(problems,
					fmt.Sprintf("%s %q is claimed by both %s and %s", field.name, v, prev, s.Key))
			}
			seen[v] = s.Key
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid setting registry:\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}

// Lookup returns the setting with the given canonical key.
func Lookup(key string) (Setting, bool) {
	s, ok := byKey[key]
	return s, ok
}

// Keys returns every canonical key, sorted.
func Keys() []string {
	all := All()
	out := make([]string, 0, len(all))
	for _, s := range all {
		out = append(out, s.Key)
	}
	sort.Strings(out)
	return out
}

// BindFlags registers the settings that belong to no single command, as
// persistent flags on the root.
//
// Call it at the point flags are wired rather than capturing All() earlier: a
// command registers its settings in its own init, and package initialization
// order puts that before the command tree is built, not before this package
// loads.
func BindFlags(fs *pflag.FlagSet) {
	for _, s := range All() {
		if len(s.Commands) == 0 {
			bind(fs, s)
		}
	}
}

// BindCommandFlags registers the settings scoped to one command path, on that
// command's own flag set. A setting naming several paths is bound to each: one
// setting with two bindings, not two settings.
func BindCommandFlags(path string, fs *pflag.FlagSet) {
	for _, s := range All() {
		if slices.Contains(s.Commands, path) {
			bind(fs, s)
		}
	}
}

// bind registers one flag with the setting's declared default, so --help
// prints it.
//
// This does not confuse a default with a supplied value: pflag sets Changed
// only from Set, never from registration, and Resolve reads a flag only when
// Changed reports true.
//
// Only bools get a typed flag. Ints and durations register as strings so that
// Setting.parse is the one thing deciding what they accept: pflag's Int reads
// base 0, so it takes "0x10" as 16 where parse rejects it, and its errors name
// no canonical key. One vocabulary across all three channels is worth more
// than the type shown in help.
func bind(fs *pflag.FlagSet, s Setting) {
	if s.Kind == KindBool {
		// An empty Default yields nil from parse; the assertion then gives
		// false, which is right for a bool with no declared default.
		def, _ := s.parse(s.Default)
		b, _ := def.(bool)
		fs.Bool(s.Flag, b, s.Usage)
		return
	}
	fs.String(s.Flag, s.Default, s.Usage)
}

// parse converts a raw string to the setting's kind, returning a message that
// names the setting rather than the raw value's type.
func (s Setting) parse(raw string) (any, error) {
	switch s.Kind {
	case KindBool:
		switch raw {
		case "true", "1":
			return true, nil
		case "false", "0", "":
			return false, nil
		}
		return nil, fmt.Errorf("%s must be true or false, got %q", s.Key, raw)
	case KindInt:
		// Atoi rather than Sscanf: Sscanf stops at the first character it
		// cannot read and reports success for the prefix, so "3x" would be 3
		// and "2.5" would be 2. pflag rejects both, and a value one channel
		// accepts and another rejects cannot round-trip through a record.
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be an integer, got %q", s.Key, raw)
		}
		return n, nil
	case KindDuration:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be a duration such as 60s or 5m, got %q", s.Key, raw)
		}
		return d, nil
	default:
		return raw, nil
	}
}
