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
	"sort"
	"time"

	"github.com/spf13/pflag"
)

// Origin is the channel a resolved value came from.
type Origin int

const (
	// OriginDefault is the setting's declared default; no channel supplied it.
	OriginDefault Origin = iota
	OriginEnv
	OriginFile
	OriginFlag
)

// Describe names the origin the way the operator would refer to it, so an
// error message points at the thing they would edit.
func (o Origin) Describe(s Setting) string {
	switch o {
	case OriginEnv:
		return s.Env
	case OriginFile:
		return "config." + s.Key
	case OriginFlag:
		return "--" + s.Flag
	default:
		return "default"
	}
}

func (o Origin) String() string {
	switch o {
	case OriginEnv:
		return "env"
	case OriginFile:
		return "file"
	case OriginFlag:
		return "flag"
	default:
		return "default"
	}
}

// Value is one setting's resolved value and the channel that supplied it.
type Value struct {
	Setting Setting
	Raw     string
	From    Origin
	// Supplied is false when Raw is the declared default. It distinguishes an
	// empty value a channel provided from one nobody set, which the Cloud SQL
	// instance setting depends on.
	Supplied bool
}

// Resolved is every setting after the channels have been layered.
type Resolved struct {
	values map[string]Value
}

// ResolveOptions carries inputs that are not themselves settings.
type ResolveOptions struct {
	// File is the parsed configuration document, or nil when --config was not
	// given.
	File *File
	// Env is the environment to read, normally Environ().
	Env map[string]string
}

// Resolve layers the channels in precedence order, lowest to highest:
//
//	environment < file < flag
//
// A channel that does not carry a setting leaves the lower layer in place. The
// file outranking the environment is deliberate: the file is the artifact an
// operator owns and reviews, so ambient state must not silently outrank it.
func Resolve(fs *pflag.FlagSet, opts ResolveOptions) (*Resolved, error) {
	all := All()
	r := &Resolved{values: make(map[string]Value, len(all))}

	for _, s := range all {
		v := Value{Setting: s, Raw: s.Default, From: OriginDefault}

		if raw, ok := opts.Env[s.Env]; ok {
			v = Value{Setting: s, Raw: raw, From: OriginEnv, Supplied: true}
		}
		if opts.File != nil {
			if raw, ok := opts.File.Get(s.Key); ok {
				v = Value{Setting: s, Raw: raw, From: OriginFile, Supplied: true}
			}
		}
		if fs != nil && fs.Changed(s.Flag) {
			raw, err := fs.GetString(s.Flag)
			if err != nil {
				// Non-string kinds are stored as their own type; read the
				// flag's printed form, which round-trips for bool and int.
				raw = fs.Lookup(s.Flag).Value.String()
			}
			v = Value{Setting: s, Raw: raw, From: OriginFlag, Supplied: true}
		}

		// A setting with no declared default that nothing supplied has no
		// value to parse. Parsing the empty default anyway would fail every
		// optional int and duration -- and because this walks the merged
		// registry, fail them for every command, not just the owning one.
		// Whether an unset setting is acceptable is Require's question.
		if v.Supplied || v.Raw != "" {
			if _, err := s.parse(v.Raw); err != nil {
				return nil, &InvalidError{Value: v, Want: kindDescription(s.Kind)}
			}
		}
		r.values[s.Key] = v
	}
	return r, nil
}

func kindDescription(k ValueKind) string {
	switch k {
	case KindBool:
		return "true or false"
	case KindInt:
		return "an integer"
	case KindDuration:
		return "a duration such as 60s or 5m"
	default:
		return "a string"
	}
}

// Value returns the resolved value for a key. The bool is false for a key that
// is not in Registry, which is a programming error rather than a user one.
func (r *Resolved) Value(key string) (Value, bool) {
	v, ok := r.values[key]
	return v, ok
}

// String returns the resolved string for key, or "" if the key is unknown.
func (r *Resolved) String(key string) string {
	v, ok := r.values[key]
	if !ok {
		return ""
	}
	return v.Raw
}

// Bool returns the resolved boolean for key.
func (r *Resolved) Bool(key string) bool {
	v, ok := r.values[key]
	if !ok {
		return false
	}
	parsed, err := v.Setting.parse(v.Raw)
	if err != nil {
		return false
	}
	b, _ := parsed.(bool)
	return b
}

// Int returns the resolved integer for key.
func (r *Resolved) Int(key string) int {
	v, ok := r.values[key]
	if !ok {
		return 0
	}
	parsed, err := v.Setting.parse(v.Raw)
	if err != nil {
		return 0
	}
	n, _ := parsed.(int)
	return n
}

// Duration returns the resolved duration for key.
func (r *Resolved) Duration(key string) time.Duration {
	v, ok := r.values[key]
	if !ok {
		return 0
	}
	parsed, err := v.Setting.parse(v.Raw)
	if err != nil {
		return 0
	}
	d, _ := parsed.(time.Duration)
	return d
}

// Supplied reports whether any channel set the key, as opposed to it holding
// its declared default.
func (r *Resolved) Supplied(key string) bool {
	v, ok := r.values[key]
	return ok && v.Supplied
}

// Require returns a RequiredError when no channel supplied key and the setting
// has no default.
func (r *Resolved) Require(key string) error {
	v, ok := r.values[key]
	if !ok {
		return fmt.Errorf("unknown setting %q", key)
	}
	if v.Raw == "" {
		return &RequiredError{Setting: v.Setting}
	}
	return nil
}

// Origins lists every setting a channel supplied, sorted by key. Used to show
// where the effective configuration came from.
func (r *Resolved) Origins() []Value {
	out := make([]Value, 0, len(r.values))
	for _, v := range r.values {
		if v.Supplied {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Setting.Key < out[j].Setting.Key })
	return out
}
