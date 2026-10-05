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
	"io"
	"sort"
)

// Report writes the settings a run is about to apply and where each came from.
//
// A value can arrive from a flag, an exported variable or a configuration
// file, and only the first of those is visible in the command the operator
// typed. Reporting on every run is what keeps an inherited variable or a
// forgotten file from changing an install silently. Errors name the channel
// too, but an install that succeeds with the wrong value never produces one.
//
// Only supplied settings are listed. The rest are counted: a reader scanning
// for what is unusual about this run does not need the declared defaults
// enumerated, and listing all of them would bury the few that matter.
// ReportOptions selects how much of the configuration is listed.
type ReportOptions struct {
	// OmitDefaults lists only the settings a channel supplied.
	//
	// The full listing is the default: the report answers "where did every
	// value come from", and a setting left out of it is one the reader has to
	// go and look up. Omitting is for callers that already know, or that have
	// more output than the configuration is worth.
	OmitDefaults bool
}

func (r *Resolved) Report(w io.Writer, opts ReportOptions) {
	supplied := r.Origins()
	fmt.Fprintf(w, "configuration: %d set, %d default\n", len(supplied), len(r.values)-len(supplied))

	rows := r.allSorted()
	if opts.OmitDefaults {
		rows = supplied
	}

	keyWidth, valWidth := 0, 0
	for _, v := range rows {
		keyWidth = max(keyWidth, len(v.Setting.Key))
		valWidth = max(valWidth, len(reported(v)))
	}
	for _, v := range rows {
		fmt.Fprintf(w, "  %-*s  %-*s  (from %s)\n",
			keyWidth, v.Setting.Key, valWidth, reported(v), v.From.Describe(v.Setting))
	}
}

// allSorted is every setting, supplied or not, by key.
func (r *Resolved) allSorted() []Value {
	out := make([]Value, 0, len(r.values))
	for _, v := range r.values {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Setting.Key < out[j].Setting.Key })
	return out
}

// Display is the value as it may be shown. A secret that something supplied
// reports that it is set without disclosing what to: the reader needs to know
// which channel to go and change, not the credential itself. One sitting on
// its declared default has no credential to hide, and reporting "<set>" for
// it would claim a credential is configured when none is.
//
// The value is returned undelimited, because the callers delimit differently:
// an error quotes it with %q, and the report pads it into a column.
func (v Value) Display() string {
	if v.Setting.Secret && v.Supplied {
		return "<set>"
	}
	return v.Raw
}

// reported is Display for the table. An empty value is shown as "" so that a
// setting something set to empty is not an empty column, which would read as
// though nothing had been printed.
func reported(v Value) string {
	if shown := v.Display(); shown != "" {
		return shown
	}
	return `""`
}
