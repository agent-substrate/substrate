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

package authz

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFormatMember(t *testing.T) {
	tests := []struct {
		name    string
		member  string
		want    string
		wantErr bool
	}{
		{name: "plain", member: "user:alice@example.com", want: "user:alice@example.com"},
		{name: "encodes reserved characters", member: "user:a:b#c d*%", want: "user:a%3Ab%23c%20d%2A%25"},
		{name: "missing prefix", member: "alice", wantErr: true},
		{name: "empty id", member: "user:", wantErr: true},
		{name: "whitespace id", member: "user:   ", wantErr: true},
		{name: "wildcard", member: "user:*", wantErr: true},
		{name: "tab", member: "user:alice\tbob", wantErr: true},
		{name: "newline", member: "user:alice\nbob", wantErr: true},
		{name: "NUL", member: "user:alice\x00", wantErr: true},
		{name: "DEL", member: "user:alice\x7f", wantErr: true},
		{name: "C1 control", member: "user:alice\u0085", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FormatMember(tc.member)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("FormatMember(%q) = %q, want error", tc.member, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("FormatMember(%q) failed: %v", tc.member, err)
			}
			if got != tc.want {
				t.Errorf("FormatMember(%q) = %q, want %q", tc.member, got, tc.want)
			}
		})
	}
}

func TestParseBootstrapOwners(t *testing.T) {
	got, err := parseBootstrapOwners([]string{"alice@example.com", " user:bob ", "alice@example.com", "a:b"})
	if err != nil {
		t.Fatalf("parseBootstrapOwners failed: %v", err)
	}
	want := map[string]struct{}{
		"user:alice@example.com": {},
		"user:bob":               {},
		"user:a%3Ab":             {},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("parseBootstrapOwners (-want +got):\n%s", diff)
	}

	for _, bad := range []string{"", "  ", "user:", "*", "user:*", "alice\tbob"} {
		if _, err := parseBootstrapOwners([]string{bad}); err == nil {
			t.Errorf("parseBootstrapOwners(%q) succeeded, want error", bad)
		}
	}
}
