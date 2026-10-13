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

package egresspolicy

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestValidatePathPrefix(t *testing.T) {
	for _, prefix := range []string{"/", "/api", "/api/", "/api/v4/projects", "/a;b/c", "/%41pi", "/~user/repo.git"} {
		if err := ValidatePathPrefix(prefix); err != nil {
			t.Errorf("ValidatePathPrefix(%q) = %v, want nil", prefix, err)
		}
	}
	for _, prefix := range []string{
		"api/", "*", "//", "/api//v4", "/api/./v4", "/api/../v4", "/..", "/.", "/api/..;/v4",
		`/api\v4`, "/api%2fv4", "/api%2Fv4", "/%2e%2e/", "/api%5cv4", "/api?x=1", "/api#x",
		"/api v4", "/api/\x00", "/café",
	} {
		if err := ValidatePathPrefix(prefix); err == nil {
			t.Errorf("ValidatePathPrefix(%q) = nil, want an error", prefix)
		}
	}
}

func TestPathPrefixMatches(t *testing.T) {
	for _, tc := range []struct {
		prefix, path string
		want         bool
	}{
		{"/api", "/api", true},
		{"/api", "/api/v4", true},
		{"/api", "/apis", false},
		{"/api/", "/api/v4", true},
		{"/api/", "/api/", true},
		{"/api/", "/api", false},
		{"/", "/anything", true},
		{"/api/v4", "/api", false},
	} {
		if got := PathPrefixMatches(tc.prefix, tc.path); got != tc.want {
			t.Errorf("PathPrefixMatches(%q, %q) = %v, want %v", tc.prefix, tc.path, got, tc.want)
		}
	}
}

// ReplaceHeadersFor picks, per header name, the entry with the longest
// matching path prefix, the entry without one otherwise.
func TestReplaceHeadersFor(t *testing.T) {
	basic := &ateapipb.CredentialHeader{Header: "Authorization", Prefix: "Basic ", CredentialUri: "ate-secret://p/basic"}
	bearer := &ateapipb.CredentialHeader{Header: "authorization", Prefix: "Bearer ", CredentialUri: "ate-secret://p/bearer", PathPrefix: "/api/"}
	v4 := &ateapipb.CredentialHeader{Header: "AUTHORIZATION", Prefix: "Token ", CredentialUri: "ate-secret://p/v4", PathPrefix: "/api/v4"}
	apiKey := &ateapipb.CredentialHeader{Header: "X-Api-Key", CredentialUri: "ate-secret://p/key", PathPrefix: "/api/"}
	effects := &ateapipb.HttpRuleEffects{ReplaceHeaders: []*ateapipb.CredentialHeader{bearer, basic, apiKey, v4}}

	for _, tc := range []struct {
		target string
		want   []*ateapipb.CredentialHeader
	}{
		{"/repo.git/info/refs?service=git-upload-pack", []*ateapipb.CredentialHeader{basic}},
		{"/api/users", []*ateapipb.CredentialHeader{bearer, apiKey}},
		{"/api/v4/projects?per_page=1", []*ateapipb.CredentialHeader{v4, apiKey}},
		{"/api/v4", []*ateapipb.CredentialHeader{v4, apiKey}},
		{"/api/v40", []*ateapipb.CredentialHeader{bearer, apiKey}},
		{"/api", []*ateapipb.CredentialHeader{basic}},
		{"/api/?/v4", []*ateapipb.CredentialHeader{bearer, apiKey}},
		// Paths an origin could resolve elsewhere get only the entry
		// without a prefix.
		{"/api/../admin", []*ateapipb.CredentialHeader{basic}},
		{"/api/v4/..;/x", []*ateapipb.CredentialHeader{basic}},
		{"/api//v4", []*ateapipb.CredentialHeader{basic}},
		{"/api/%2e%2e/admin", []*ateapipb.CredentialHeader{basic}},
		{`/api\v4`, []*ateapipb.CredentialHeader{basic}},
		{"https://git.example.com/api/v4", []*ateapipb.CredentialHeader{basic}},
		{"", []*ateapipb.CredentialHeader{basic}},
	} {
		got := ReplaceHeadersFor(effects, tc.target)
		if len(got) != len(tc.want) {
			t.Errorf("ReplaceHeadersFor(%q) = %v, want %v", tc.target, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("ReplaceHeadersFor(%q)[%d] = %v, want %v", tc.target, i, got[i], tc.want[i])
			}
		}
	}

	if got := ReplaceHeadersFor(nil, "/api/"); len(got) != 0 {
		t.Errorf("ReplaceHeadersFor(nil) = %v, want none", got)
	}
}
