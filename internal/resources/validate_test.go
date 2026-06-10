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

package resources

import (
	"strings"
	"testing"
)

func TestValidateActorRef(t *testing.T) {
	const okNS, okTmpl, okID = "ate-demo", "counter", "counter-1"

	tests := []struct {
		name         string
		ns, tmpl, id string
		wantErr      bool
	}{
		{"all valid", okNS, okTmpl, okID, false},
		{"uuid id valid", okNS, okTmpl, "422938ba-8860-4983-a25d-d6bcb0a69d4e", false},

		// Label vs subdomain distinction: template names are DNS-1123
		// subdomains (dots allowed); namespaces and actor IDs are labels.
		{"dotted template valid (subdomain)", okNS, "probe.v1", okID, false},
		{"dotted namespace invalid (label)", "ate.demo", okTmpl, okID, true},
		{"dotted id invalid (label)", okNS, okTmpl, "probe.alpha", true},

		{"id traversal", okNS, okTmpl, "..", true},
		{"id separator", okNS, okTmpl, "a/b", true},
		{"id empty", okNS, okTmpl, "", true},
		{"id uppercase", okNS, okTmpl, "Counter", true},
		{"id too long", okNS, okTmpl, strings.Repeat("a", 64), true},

		{"namespace separator", "a/b", okTmpl, okID, true},
		{"namespace traversal", "..", okTmpl, okID, true},
		{"namespace empty", "", okTmpl, okID, true},
		{"template separator", okNS, "a/b", okID, true},
		{"template traversal", okNS, "..", okID, true},

		// The names join into one <ns>:<tmpl>:<id> path component, capped at
		// 255 bytes even when each name is individually valid.
		{"combined fits filename limit", strings.Repeat("a", 63), strings.Repeat("b", 120), strings.Repeat("c", 63), false},
		{"combined exceeds filename limit", strings.Repeat("a", 63), strings.Repeat("b", 253), strings.Repeat("c", 63), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateActorRef(tt.ns, tt.tmpl, tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateActorRef(%q, %q, %q) err = %v, wantErr %v", tt.ns, tt.tmpl, tt.id, err, tt.wantErr)
			}
		})
	}
}

func TestValidateAteomUID(t *testing.T) {
	tests := []struct {
		name    string
		uid     string
		wantErr bool
	}{
		{"uuid valid", "422938ba-8860-4983-a25d-d6bcb0a69d4e", false},
		{"separator", "a/b", true},
		{"traversal", "..", true},
		{"empty", "", true},
		{"uppercase", "Pod-UID", true},
		{"too long", strings.Repeat("a", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateAteomUID(tt.uid); (err != nil) != tt.wantErr {
				t.Errorf("ValidateAteomUID(%q) err = %v, wantErr %v", tt.uid, err, tt.wantErr)
			}
		})
	}
}

func TestValidateContainerNames(t *testing.T) {
	tests := []struct {
		name    string
		names   []string
		wantErr bool
	}{
		{"no containers", nil, false},
		{"single valid", []string{"worker"}, false},
		{"multiple valid", []string{"worker", "sidecar"}, false},
		{"separator", []string{"a/b"}, true},
		{"traversal", []string{".."}, true},
		{"empty name", []string{""}, true},
		{"uppercase", []string{"Worker"}, true},
		{"reserved pause", []string{"pause"}, true},
		{"reserved pause among valid", []string{"worker", "pause"}, true},
		{"duplicate", []string{"worker", "worker"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateContainerNames(tt.names); (err != nil) != tt.wantErr {
				t.Errorf("ValidateContainerNames(%v) err = %v, wantErr %v", tt.names, err, tt.wantErr)
			}
		})
	}
}

func TestValidateRunscHash(t *testing.T) {
	const valid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	tests := []struct {
		name    string
		hash    string
		wantErr bool
	}{
		{"valid lowercase", valid, false},
		{"valid uppercase", strings.ToUpper(valid), false},
		{"empty", "", true},
		{"too short", "abc123", true},
		{"too long", valid + "00", true},
		{"separator", strings.Repeat("a", 60) + "/../", true},
		{"non-hex", strings.Repeat("g", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRunscHash(tt.hash); (err != nil) != tt.wantErr {
				t.Errorf("ValidateRunscHash(%q) err = %v, wantErr %v", tt.hash, err, tt.wantErr)
			}
		})
	}
}

func TestValidateSnapshotURIPrefix(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{"valid with trailing slash", "gs://bucket/actors/1234/snapshots/5678/", false},
		{"valid without path", "gs://bucket", false},
		// Scheme is storage-backend policy, not validated here.
		{"valid alternate scheme", "s3://bucket/path", false},
		{"empty", "", true},
		{"missing bucket", "gs://", true},
		{"no scheme or bucket", "bucket/path", true},
		{"unparseable", "://bucket", true},
		// Appended object names must not be swallowed by URL components.
		{"query", "gs://bucket/path?x=1", true},
		{"fragment", "gs://bucket/path#frag", true},
		{"userinfo", "gs://user@bucket/path", true},
		// Opaque form (no //) parses with an empty host, so it is rejected
		// on either the bucket or the opaque check.
		{"opaque", "gs:bucket/path", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateSnapshotURIPrefix(tt.prefix); (err != nil) != tt.wantErr {
				t.Errorf("ValidateSnapshotURIPrefix(%q) err = %v, wantErr %v", tt.prefix, err, tt.wantErr)
			}
		})
	}
}
