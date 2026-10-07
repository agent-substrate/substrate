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
func TestRequiredErrorCoversEverySetting(t *testing.T) {
	for _, s := range Registry {
		t.Run(s.Key, func(t *testing.T) {
			got := (&RequiredError{Setting: s}).Error()
			want := []string{s.Env, "config." + s.Key}
			// A secret has no flag to name.
			if s.Flag != "" {
				want = append(want, "--"+s.Flag)
			}
			for _, want := range want {
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

// Every rejection an operator can trigger has to name the channel the value
// came from. A message naming only a flag sends a reader who configured by
// file or by export to something they never typed.
func TestValidationErrorsNameTheSupplyingChannel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		env    map[string]string
		wantIn []string
	}{
		{
			name:   "extproc service is malformed",
			env:    map[string]string{"ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE": "nope"},
			wantIn: []string{"atenet.egress.additionalExtprocService", "<namespace>/<service>:<port>", `"nope"`, "ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE"},
		},
		{
			name:   "extproc port is out of range",
			env:    map[string]string{"ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE": "ns/svc:99999"},
			wantIn: []string{"atenet.egress.additionalExtprocService", "1-65535", "ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE"},
		},
		{
			name:   "image tag without a repository",
			env:    map[string]string{"ATE_IMAGE_TAG": "v1"},
			wantIn: []string{"images.tag", "ATE_IMAGE_TAG", "images.repo", "the tag names nothing"},
		},
		{
			name:   "image repository without a tag",
			env:    map[string]string{"ATE_IMAGE_REPO": "example.com/substrate"},
			wantIn: []string{"images.repo", "ATE_IMAGE_REPO", "images.tag", "needs a tag"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load(Options{})
			if err == nil {
				t.Fatal("Load() = nil error, want one")
			}
			for _, want := range tc.wantIn {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Load() = %q, missing %q", err, want)
				}
			}
			// The pre-uniform-config messages named a flag and nothing else.
			if strings.HasPrefix(err.Error(), "--") {
				t.Errorf("Load() = %q, which leads with a flag and names no channel", err)
			}
		})
	}
}

// Every rejection names the three channels a setting can come from, so a
// reader who used the file is not sent to look for a flag. The
// credential-provider message predates the file channel and names only two.
func TestCredentialProviderErrorNamesTheFileChannel(t *testing.T) {
	useFixtures(t)
	cfg := &Config{}
	_, err := cfg.CredentialProvider()
	if err == nil {
		t.Fatal("CredentialProvider() with nothing set returned no error")
	}
	for _, want := range []string{
		"--credential-provider",
		"ATE_CREDENTIAL_PROVIDER",
		"config.atenet.egress.credentialProvider",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q does not name %s", err, want)
		}
	}
}
