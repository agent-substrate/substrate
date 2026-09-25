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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// The capabilities e2e assertions are only as trustworthy as this decoder and
// its name table, and that suite needs a cluster to run. These cases pin both
// against masks whose meaning is known independently.
func TestDecodeCapMask(t *testing.T) {
	tests := []struct {
		name string
		mask string
		want []string
	}{{
		name: "no capabilities",
		mask: "0000000000000000",
		want: []string{},
	}, {
		// The container-runtime default set (docker, containerd/CRI). Decoding
		// this to exactly its 14 well-known members is what validates the bit
		// positions in capabilityNames.
		name: "container-runtime default set",
		mask: "00000000a80425fb",
		want: []string{
			"CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "KILL",
			"SETGID", "SETUID", "SETPCAP", "NET_BIND_SERVICE", "NET_RAW",
			"SYS_CHROOT", "MKNOD", "AUDIT_WRITE", "SETFCAP",
		},
	}, {
		// atelet's default set: KILL (5), NET_BIND_SERVICE (10), AUDIT_WRITE (29).
		name: "atelet default set",
		mask: "0000000020000420",
		want: []string{"KILL", "NET_BIND_SERVICE", "AUDIT_WRITE"},
	}, {
		name: "single capability",
		mask: "0000000000000400",
		want: []string{"NET_BIND_SERVICE"},
	}, {
		// A bit past the end of the table must still be reported, so a kernel
		// newer than this table yields a readable diff, not a silent omission.
		name: "unknown capability bit",
		mask: "0000020000000000",
		want: []string{"CAP_41"},
	}, {
		name: "leading and trailing whitespace is tolerated",
		mask: "  0000000000000020\n",
		want: []string{"KILL"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeCapMask(tt.mask)
			if err != nil {
				t.Fatalf("decodeCapMask(%q) failed: %v", tt.mask, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("decodeCapMask(%q) =\n  %v\nwant:\n  %v", tt.mask, got, tt.want)
			}
		})
	}
}

func TestParseFetchHeaders(t *testing.T) {
	tests := []struct {
		name    string
		params  []string
		want    http.Header
		wantErr bool
	}{{
		name:   "no parameters",
		params: nil,
		want:   http.Header{},
	}, {
		name:   "name and value",
		params: []string{"Authorization:Bearer x"},
		want:   http.Header{"Authorization": {"Bearer x"}},
	}, {
		// Only the first colon separates; the value keeps the rest verbatim.
		name:   "value containing a colon",
		params: []string{"X-Test:a:b"},
		want:   http.Header{"X-Test": {"a:b"}},
	}, {
		name:   "repeated name keeps every value",
		params: []string{"x-test:1", "X-Test:2"},
		want:   http.Header{"X-Test": {"1", "2"}},
	}, {
		name:    "no colon",
		params:  []string{"no-colon"},
		wantErr: true,
	}, {
		name:    "empty name",
		params:  []string{":empty-name"},
		wantErr: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFetchHeaders(tt.params)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseFetchHeaders(%q) = %v, want an error", tt.params, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFetchHeaders(%q) failed: %v", tt.params, err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("parseFetchHeaders(%q) mismatch (-want +got):\n%s", tt.params, diff)
			}
		})
	}
}

// doFetch drives the fetch handler at an origin URL and decodes its JSON
// reply. roots=system keeps the handler off the projected trust bundle,
// which does not exist outside a cluster.
func doFetch(t *testing.T, origin string, headerParams ...string) map[string]string {
	t.Helper()
	query := url.Values{"url": {origin}, "roots": {"system"}}
	for _, h := range headerParams {
		query.Add("header", h)
	}
	req := httptest.NewRequest(http.MethodGet, "/fetch?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	fetch(rec, req)
	resp := map[string]string{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding fetch response %q: %v", rec.Body.String(), err)
	}
	return resp
}

// The suites' credential-injection assertions live in the response body an
// origin echoes back, so fetch must return it — along with the request
// headers set from ?header= parameters, which is how a suite pre-seeds a
// header the gateway should overwrite.
func TestFetchReturnsBodyAndSetsHeaders(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("auth=" + r.Header.Get("Authorization")))
	}))
	defer origin.Close()

	resp := doFetch(t, origin.URL, "Authorization:Bearer seeded")
	if resp["error"] != "" {
		t.Fatalf("fetch failed: %s", resp["error"])
	}
	if resp["status"] != "200" {
		t.Errorf("status = %q, want 200", resp["status"])
	}
	if resp["body"] != "auth=Bearer seeded" {
		t.Errorf("body = %q, want %q", resp["body"], "auth=Bearer seeded")
	}
}

// A followed cross-scheme redirect would silently hop between the gateway's
// cleartext and TLS legs, so fetch must report the first response instead.
func TestFetchDoesNotFollowRedirects(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			t.Error("fetch followed the redirect")
		}
		http.Redirect(w, r, "/target", http.StatusFound)
	}))
	defer origin.Close()

	resp := doFetch(t, origin.URL)
	if resp["error"] != "" {
		t.Fatalf("fetch failed: %s", resp["error"])
	}
	if resp["status"] != "302" {
		t.Errorf("status = %q, want 302", resp["status"])
	}
}

// A malformed ?header= fails the fetch before anything is sent.
func TestFetchRejectsMalformedHeader(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("fetch sent the request despite a malformed header parameter")
	}))
	defer origin.Close()

	resp := doFetch(t, origin.URL, "no-colon")
	if !strings.Contains(resp["error"], "not <name>:<value>") {
		t.Errorf("error = %q, want the malformed-header error", resp["error"])
	}
	if resp["status"] != "" {
		t.Errorf("status = %q, want none", resp["status"])
	}
}

func TestFetchTruncatesBody(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxFetchBody+1)))
	}))
	defer origin.Close()

	resp := doFetch(t, origin.URL)
	if got := len(resp["body"]); got != maxFetchBody {
		t.Errorf("len(body) = %d, want %d", got, maxFetchBody)
	}
}

func TestDecodeCapMaskInvalid(t *testing.T) {
	if _, err := decodeCapMask("nothex"); err == nil {
		t.Error("decodeCapMask(\"nothex\") succeeded, want an error")
	}
}

// capabilityNames is indexed by capability value, so a wrong length means the
// table has drifted from <linux/capability.h> and every decoded name past the
// gap would be wrong.
func TestCapabilityNamesTable(t *testing.T) {
	const wantLen = 41 // CAP_CHOWN (0) .. CAP_CHECKPOINT_RESTORE (40)
	if len(capabilityNames) != wantLen {
		t.Errorf("len(capabilityNames) = %d, want %d", len(capabilityNames), wantLen)
	}
	for i, name := range capabilityNames {
		if name == "" {
			t.Errorf("capabilityNames[%d] is empty", i)
		}
	}
}
