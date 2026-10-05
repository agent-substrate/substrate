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
	"bytes"
	"strconv"
	"strings"
	"testing"
)

// A value from a file or an exported variable is invisible in the command the
// operator typed, so every channel has to be named on a run that succeeds --
// an install that works with the wrong value never produces an error.
func TestReportNamesEveryChannel(t *testing.T) {
	useFixtures(t)
	fs := flagsWith(t, map[string]string{"fixture-name": "from-flag"})
	file := &File{Path: "install.yaml", values: map[string]string{"fixture.count": "9"}}
	r, err := Resolve(fs, ResolveOptions{
		Env:  map[string]string{"FIXTURE_MODE": "beta"},
		File: file,
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	var buf bytes.Buffer
	r.Report(&buf, ReportOptions{})
	got := buf.String()

	for _, want := range []string{
		"fixture.name", "from-flag", "--fixture-name",
		"fixture.mode", "beta", "FIXTURE_MODE",
		"fixture.count", "9", "config.fixture.count",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Report() is missing %q:\n%s", want, got)
		}
	}
}

// The summary counts both halves, and the two numbers must account for the
// whole registry however much of it is listed.
func TestReportCountsSuppliedAndDefaulted(t *testing.T) {
	useFixtures(t)
	r, err := Resolve(nil, ResolveOptions{Env: map[string]string{"FIXTURE_NAME": "just-one"}})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	// The summary is the same either way; only the listing below it differs.
	var full, short bytes.Buffer
	r.Report(&full, ReportOptions{})
	r.Report(&short, ReportOptions{OmitDefaults: true})

	want := "configuration: 1 set, " + strconv.Itoa(len(All())-1) + " default"
	for _, b := range []*bytes.Buffer{&full, &short} {
		if first := strings.SplitN(b.String(), "\n", 2)[0]; first != want {
			t.Errorf("first line = %q, want %q", first, want)
		}
	}

	buf := short
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("Report(OmitDefaults) printed %d lines, want 2 (the summary and the one set value):\n%s",
			len(lines), buf.String())
	}
	if n := strings.Count(full.String(), "\n"); n != len(All())+1 {
		t.Errorf("Report() printed %d lines, want %d (every setting plus the summary)", n, len(All())+1)
	}
	if !strings.Contains(lines[1], "just-one") {
		t.Errorf("second line = %q, want the value that was set", lines[1])
	}
}

// The report runs on every install, so a credential in it would be a
// credential in every terminal and CI log that ever ran one.
func TestReportNeverPrintsASecretValue(t *testing.T) {
	useFixtures(t)
	env := map[string]string{}
	for _, s := range All() {
		if s.Secret {
			env[s.Env] = "SENSITIVE-" + s.Key
		}
	}
	if len(env) == 0 {
		t.Fatal("no Secret settings registered; this test would pass vacuously")
	}

	r, err := Resolve(nil, ResolveOptions{Env: env})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	var buf bytes.Buffer
	r.Report(&buf, ReportOptions{})
	got := buf.String()

	if strings.Contains(got, "SENSITIVE") {
		t.Errorf("Report() disclosed a secret value:\n%s", got)
	}
	// Absent entirely would be worse than masked: the reader could not tell
	// the setting had been supplied at all, nor which channel to go and fix.
	for key, value := range env {
		if !strings.Contains(got, key) {
			t.Errorf("Report() omits the channel %q that supplied %q", key, value)
		}
	}
	if !strings.Contains(got, "<set>") {
		t.Errorf("Report() does not mark the secret as set:\n%s", got)
	}
}

func TestValueDisplay(t *testing.T) {
	useFixtures(t)
	secret := setting(t, "fixture.secret")
	plain := setting(t, "fixture.name")
	for _, tc := range []struct {
		name string
		v    Value
		want string
	}{
		{name: "plain", v: Value{Setting: plain, Raw: "a-value"}, want: "a-value"},
		{name: "supplied secret", v: Value{Setting: secret, Raw: "postgres://u:p@h/d", Supplied: true}, want: "<set>"},
		{name: "unsupplied secret", v: Value{Setting: secret}, want: ""},
		{name: "explicitly empty", v: Value{Setting: plain, Raw: ""}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.v.Display(); got != tc.want {
				t.Errorf("Display() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Every setting is listed unless the caller asks otherwise, so the report
// answers where each value came from without the reader looking any up.
func TestReportListsEverySettingByDefault(t *testing.T) {
	useFixtures(t)
	r, err := Resolve(nil, ResolveOptions{Env: map[string]string{"FIXTURE_NAME": "chosen"}})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	var short, long bytes.Buffer
	r.Report(&short, ReportOptions{OmitDefaults: true})
	r.Report(&long, ReportOptions{})

	shortLines := strings.Count(short.String(), "\n")
	longLines := strings.Count(long.String(), "\n")
	if want := len(All()) + 1; longLines != want {
		t.Errorf("the long form printed %d lines, want %d (every setting plus the summary)", longLines, want)
	}
	if shortLines >= longLines {
		t.Errorf("the short form printed %d lines and the long one %d", shortLines, longLines)
	}

	// Both forms agree about the summary and about what was supplied.
	for _, form := range []struct {
		name string
		got  string
	}{{"short", short.String()}, {"long", long.String()}} {
		if !strings.Contains(form.got, "chosen") {
			t.Errorf("the %s form omits the supplied value", form.name)
		}
	}
	for _, want := range []string{"fixture.mode", "alpha", "(from default)"} {
		if !strings.Contains(long.String(), want) {
			t.Errorf("the long form is missing %q:\n%s", want, long.String())
		}
	}
}

// Listing every setting must not disclose a credential, and must not claim
// one is configured when nothing supplied it.
func TestReportKeepsSecretsWithheldWhenListingEverything(t *testing.T) {
	useFixtures(t)
	secretKey := "fixture.secret"

	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantIn  string
		wantOut string
	}{
		{
			name:    "supplied",
			env:     map[string]string{"FIXTURE_SECRET": "SENSITIVE"},
			wantIn:  "<set>",
			wantOut: "SENSITIVE",
		},
		{
			name:   "not supplied",
			wantIn: secretKey,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := Resolve(nil, ResolveOptions{Env: tc.env})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			var buf bytes.Buffer
			r.Report(&buf, ReportOptions{})
			got := buf.String()

			if tc.wantOut != "" && strings.Contains(got, tc.wantOut) {
				t.Errorf("Report() disclosed the credential:\n%s", got)
			}
			if !strings.Contains(got, tc.wantIn) {
				t.Errorf("Report() is missing %q:\n%s", tc.wantIn, got)
			}
			if tc.name == "not supplied" && strings.Contains(got, "<set>") {
				t.Errorf("Report() says a secret nothing supplied is set:\n%s", got)
			}
		})
	}
}
