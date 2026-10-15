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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSeedVolumeFixture(t *testing.T) {
	root := t.TempDir()
	body, _ := json.Marshal(map[string]string{"root": root, "value": "actor-a"})
	w := httptest.NewRecorder()
	seedVolumeFixture(w, httptest.NewRequest(http.MethodPost, "/volume-fixture", bytes.NewReader(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("seed: %d %s", w.Code, w.Body.String())
	}
	p := &volumeStateProbe{}
	volumePrime(t, p, root)
	got := volumeObserve(t, p)
	if got.Cached != "actor-a" || got.Held.Data != "actor-a" || got.Unlinked.Data != "actor-a" || !got.Negative.NotExist {
		t.Fatalf("fixture contents: %+v", got)
	}
}

func TestSeedVolumeFixtureRejectsUnsafeRoot(t *testing.T) {
	for _, root := range []string{"", "/", "relative"} {
		body, _ := json.Marshal(map[string]string{"root": root, "value": "a"})
		w := httptest.NewRecorder()
		seedVolumeFixture(w, httptest.NewRequest(http.MethodPost, "/volume-fixture", bytes.NewReader(body)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("root %q: %d", root, w.Code)
		}
	}
}

func TestRuntimeBootIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, kind, kernel, cmdline, want string
		bad                               bool
	}{
		{"microvm eager marker", "microvm", "lazy", "quiet ate.boot_id=eager", "eager", false},
		{"default microvm", "", "lazy", "ate.boot_id=eager", "eager", false},
		{"microvm must not fall back", "microvm", "lazy", "", "", true},
		{"gvisor proc identity", "gvisor", "sandbox", "", "sandbox", false},
		{"missing gvisor identity", "gvisor", "", "", "", true},
		{"unsupported runtime", "other", "some", "ate.boot_id=some", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := runtimeBootIdentity(tc.kind, tc.kernel, tc.cmdline)
			if got != tc.want || (err != nil) != tc.bad {
				t.Fatalf("got %q, %v", got, err)
			}
		})
	}
}
