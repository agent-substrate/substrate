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
)

// The required-setting message must name all three channels. A reader who
// configures by file and is told to export a variable is sent to the wrong
// place.
func TestRequiredErrorNamesEveryChannel(t *testing.T) {
	useFixtures(t)
	got := (&RequiredError{Setting: setting(t, "fixture.nested.deep.key")}).Error()
	want := "at least one of FIXTURE_NESTED_DEEP_KEY, " +
		"config.fixture.nested.deep.key or " +
		"--fixture-nested-deep-key must be set, got none"
	if got != want {
		t.Errorf("Error() =\n  %q\nwant\n  %q", got, want)
	}
}

// Every setting must produce a message naming its own three channels, so the
// format cannot rot for settings nobody hand-checked.
// TODO: vacuous until Registry declares settings. It is the check that the
// message format cannot rot for a setting nobody hand-wrote a case for, so it
// only earns its place once there are settings to cover.
func TestRequiredErrorCoversEverySetting(t *testing.T) {
	for _, s := range Registry {
		t.Run(s.Key, func(t *testing.T) {
			got := (&RequiredError{Setting: s}).Error()
			for _, want := range []string{s.Env, "config." + s.Key, "--" + s.Flag} {
				if !strings.Contains(got, want) {
					t.Errorf("Error() = %q, missing %q", got, want)
				}
			}
		})
	}
}

func TestInvalidErrorNamesTheSupplyingChannel(t *testing.T) {
	useFixtures(t)
	s := setting(t, "fixture.mode")
	for _, tc := range []struct {
		name   string
		origin Origin
		wantIn string
	}{
		{name: "from environment", origin: OriginEnv, wantIn: "FIXTURE_MODE"},
		{name: "from file", origin: OriginFile, wantIn: "config.fixture.mode"},
		{name: "from flag", origin: OriginFlag, wantIn: "--fixture-mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &InvalidError{
				Value: Value{Setting: s, Raw: "gamma", From: tc.origin},
				Want:  "alpha or beta",
			}
			got := err.Error()
			for _, want := range []string{"fixture.mode", "alpha or beta", "gamma", tc.wantIn} {
				if !strings.Contains(got, want) {
					t.Errorf("Error() = %q, missing %q", got, want)
				}
			}
		})
	}
}

// A conflict names both settings and where each came from, because the fix is
// to edit one of the two and the reader must know which file or variable holds
// it.
func TestConflictErrorNamesBothSides(t *testing.T) {
	useFixtures(t)
	err := &ConflictError{
		A:   Value{Setting: setting(t, "fixture.enabled"), Raw: "true", From: OriginFile},
		B:   Value{Setting: setting(t, "fixture.mode"), Raw: "beta", From: OriginEnv},
		Why: "the feature requires mode alpha",
	}
	got := err.Error()
	for _, want := range []string{
		"fixture.enabled", "true", "config.fixture.enabled",
		"fixture.mode", "beta", "FIXTURE_MODE",
		"the feature requires mode alpha",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Error() = %q, missing %q", got, want)
		}
	}
}

// A rejected credential must not reach the terminal or the CI log. The
// message still has to name the setting and the channel, because that is what
// the reader needs in order to go and fix it -- only the value is withheld,
// the way Report already does it.
func TestErrorMessagesWithholdSecretValues(t *testing.T) {
	secret := Setting{
		Key: "scratch.secret", Env: "SCRATCH_SECRET", Flag: "scratch-secret",
		Kind: KindString, Secret: true, Usage: "a credential",
	}
	v := Value{Setting: secret, Raw: "postgres://u:hunter2@h/db", From: OriginEnv, Supplied: true}
	other := Value{
		Setting: Setting{Key: "scratch.other", Env: "SCRATCH_OTHER", Flag: "scratch-other"},
		Raw:     "x", From: OriginFlag,
	}

	for _, tc := range []struct {
		name   string
		got    string
		wantIn []string
	}{
		{
			name:   "InvalidError",
			got:    (&InvalidError{Value: v, Want: "a DSN"}).Error(),
			wantIn: []string{"scratch.secret", "SCRATCH_SECRET", "a DSN"},
		},
		{
			name:   "ConflictError",
			got:    (&ConflictError{A: v, B: other, Why: "they disagree"}).Error(),
			wantIn: []string{"scratch.secret", "SCRATCH_SECRET", "they disagree"},
		},
		{name: "Display", got: v.Display()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.got, "hunter2") {
				t.Errorf("%q discloses the credential", tc.got)
			}
			for _, want := range tc.wantIn {
				if !strings.Contains(tc.got, want) {
					t.Errorf("%q is missing %q; only the value may be withheld", tc.got, want)
				}
			}
		})
	}
}

func TestResolvedRequire(t *testing.T) {
	useFixtures(t)
	for _, tc := range []struct {
		name    string
		env     map[string]string
		key     string
		wantErr bool
	}{
		{name: "supplied", env: map[string]string{"FIXTURE_NAME": "n"}, key: "fixture.name"},
		{name: "absent and no default", key: "fixture.name", wantErr: true},
		{name: "absent but defaulted", key: "fixture.mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := Resolve(flagsWith(t, nil), ResolveOptions{Env: tc.env})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			got := r.Require(tc.key)
			if tc.wantErr {
				if got == nil {
					t.Fatal("Require() = nil, want a RequiredError")
				}
				var re *RequiredError
				if !asRequiredError(got, &re) {
					t.Fatalf("Require() = %T, want *RequiredError", got)
				}
				return
			}
			if got != nil {
				t.Errorf("Require() = %v, want nil", got)
			}
		})
	}
}

func asRequiredError(err error, target **RequiredError) bool {
	re, ok := err.(*RequiredError)
	if ok {
		*target = re
	}
	return ok
}
