//go:build linux

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

package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
)

func TestChSocketCandidates(t *testing.T) {
	const uid = "actor-123"

	for _, tc := range []struct {
		name string
		ra   *runningActor
		want []string
	}{
		{
			name: "with recorded apiSocket",
			ra:   &runningActor{apiSocket: "/custom/path/clh.sock"},
			want: []string{"/custom/path/clh.sock"},
		},
		{
			name: "nil runningActor fallback",
			ra:   nil,
			want: []string{kata.CLHSocketPath(uid), kata.RestoredCLHSocketPath(uid)},
		},
		{
			name: "empty apiSocket fallback",
			ra:   &runningActor{apiSocket: ""},
			want: []string{kata.CLHSocketPath(uid), kata.RestoredCLHSocketPath(uid)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := chSocketCandidates(uid, tc.ra); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("chSocketCandidates(%q, %v) = %v, want %v", uid, tc.ra, got, tc.want)
			}
		})
	}
}

func TestFirstExistingPath(t *testing.T) {
	for _, tc := range []struct {
		name       string
		candidates []string
		existing   []string
		want       string
	}{
		{
			name:       "nil candidates",
			candidates: nil,
			want:       "",
		},
		{
			name:       "none existing returns first candidate",
			candidates: []string{"sock1", "sock2"},
			want:       "sock1",
		},
		{
			name:       "second candidate exists",
			candidates: []string{"sock1", "sock2"},
			existing:   []string{"sock2"},
			want:       "sock2",
		},
		{
			name:       "first candidate takes priority when both exist",
			candidates: []string{"sock1", "sock2"},
			existing:   []string{"sock1", "sock2"},
			want:       "sock1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.existing {
				if err := os.WriteFile(filepath.Join(dir, f), nil, 0o600); err != nil {
					t.Fatalf("failed to create dummy file: %v", err)
				}
			}
			var fullCandidates []string
			if tc.candidates != nil {
				fullCandidates = make([]string, len(tc.candidates))
				for i, c := range tc.candidates {
					fullCandidates[i] = filepath.Join(dir, c)
				}
			}
			want := ""
			if tc.want != "" {
				want = filepath.Join(dir, tc.want)
			}
			if got := firstExistingPath(fullCandidates); got != want {
				t.Errorf("firstExistingPath(%v) = %q, want %q", fullCandidates, got, want)
			}
		})
	}
}
