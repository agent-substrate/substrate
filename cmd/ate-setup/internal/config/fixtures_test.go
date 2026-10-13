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

import "testing"

// Registry is empty in this package until the settings are moved into it, and
// the tests here are about the mechanism rather than about which settings the
// installer declares. Each one registers the settings it needs.
//
// Keeping them separate is worth doing even once Registry is filled: a test
// for the layering or the document format should not start failing because a
// setting it borrowed as a fixture was renamed.

// useSettings registers settings for one test and removes them afterwards.
// Registration mutates package state, which is what it is for; a test that
// left it behind would change what every later test resolves.
func useSettings(t *testing.T, settings ...Setting) {
	t.Helper()
	prev := scoped
	RegisterCommand(settings...)
	t.Cleanup(func() {
		for _, s := range settings {
			delete(byKey, s.Key)
		}
		scoped = prev
	})
}

// fixtureSettings is one setting of every kind, plus the three properties the
// mechanism treats specially: a declared default, a secret, and a key that
// nests more than one level deep.
func fixtureSettings() []Setting {
	return []Setting{
		{
			Key: "fixture.name", Env: "FIXTURE_NAME", Flag: "fixture-name",
			Kind: KindString, Usage: "a string with no default",
		},
		{
			Key: "fixture.mode", Env: "FIXTURE_MODE", Flag: "fixture-mode",
			Kind: KindString, Default: "alpha", Usage: "a string with a default",
		},
		{
			Key: "fixture.enabled", Env: "FIXTURE_ENABLED", Flag: "fixture-enabled",
			Kind: KindBool, Default: "false", Usage: "a bool",
		},
		{
			Key: "fixture.count", Env: "FIXTURE_COUNT", Flag: "fixture-count",
			Kind: KindInt, Default: "1", Usage: "an int",
		},
		{
			Key: "fixture.timeout", Env: "FIXTURE_TIMEOUT", Flag: "fixture-timeout",
			Kind: KindDuration, Default: "1m0s", Usage: "a duration",
		},
		{
			Key: "fixture.secret", Env: "FIXTURE_SECRET", Flag: "fixture-secret",
			Kind: KindString, Secret: true, Usage: "a secret",
		},
		{
			Key: "fixture.nested.deep.key", Env: "FIXTURE_NESTED_DEEP_KEY",
			Flag: "fixture-nested-deep-key", Kind: KindString, Usage: "a nested key",
		},
	}
}

// useFixtures registers the standard set.
func useFixtures(t *testing.T) {
	t.Helper()
	useSettings(t, fixtureSettings()...)
}

// setting returns a fixture by key, for a test that needs the declaration
// rather than the value.
func setting(t *testing.T, key string) Setting {
	t.Helper()
	s, ok := Lookup(key)
	if !ok {
		t.Fatalf("fixture %q is not registered; call useFixtures first", key)
	}
	return s
}
