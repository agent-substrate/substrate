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
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/internal/installdefaults"
)

func testSystemPrincipals() SystemPrincipals {
	return SystemPrincipals{
		ControllerSPIFFEID:       installdefaults.ControllerSPIFFEID(installdefaults.SystemNamespace),
		EgressGatewaySPIFFEIDs:   []string{installdefaults.EgressSPIFFEID(installdefaults.SystemNamespace)},
		IngressRouterSPIFFEID:    installdefaults.RouterSPIFFEID(installdefaults.SystemNamespace),
		AteletSPIFFEID:           installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
		APIServerSidecarSPIFFEID: installdefaults.APIServerSPIFFEID(installdefaults.SystemNamespace),
	}
}

func TestFormatMember(t *testing.T) {
	tests := []struct {
		name    string
		member  string
		want    string
		wantErr bool
	}{
		{name: "plain", member: "user:alice@example.com", want: "user:alice@example.com"},
		{name: "encodes reserved characters", member: "user:a:b#c d*%", want: "user:a%3Ab%23c%20d%2A%25"},
		{name: "encoded exactly 512 bytes", member: "user:" + strings.Repeat("a:", 126) + "abc", want: "user:" + strings.Repeat("a%3A", 126) + "abc"},
		{name: "encoded 513 bytes", member: "user:" + strings.Repeat("a:", 127), wantErr: true},
		{name: "encoded exceeds 512 bytes", member: "user:" + strings.Repeat("a:", 128), wantErr: true},
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

// TestFormatMemberEncodedLengthBoundary checks the encoded-length guard at
// exactly maxTupleUserLength and one byte over. The fixtures' encoded lengths
// are asserted so the boundary arithmetic is self-checking: each "a:" expands
// to "a%3A" (4 bytes encoded, 2 bytes raw).
func TestFormatMemberEncodedLengthBoundary(t *testing.T) {
	atLimit := "user:" + strings.Repeat("a:", 126) + "abc" // 5 + 126*4 + 3 = 512 encoded
	overLimit := "user:" + strings.Repeat("a:", 127)       // 5 + 127*4 = 513 encoded
	for _, tc := range []struct {
		member     string
		wantLen    int
		wantAccept bool
	}{
		{member: atLimit, wantLen: maxTupleUserLength, wantAccept: true},
		{member: overLimit, wantLen: maxTupleUserLength + 1, wantAccept: false},
	} {
		if id, _ := strings.CutPrefix(tc.member, "user:"); len(formatUser(id)) != tc.wantLen {
			t.Fatalf("fixture %q encodes to %d bytes, want %d", tc.member, len(formatUser(id)), tc.wantLen)
		}
		got, err := FormatMember(tc.member)
		if tc.wantAccept && got != formatUser(strings.TrimPrefix(tc.member, "user:")) {
			t.Fatalf("FormatMember(...) = %q, want the encoded member", got)
		}
		if tc.wantAccept && err != nil {
			t.Errorf("FormatMember(%d-byte encoded member) failed: %v", tc.wantLen, err)
		}
		if !tc.wantAccept && err == nil {
			t.Errorf("FormatMember(%d-byte encoded member) succeeded, want error", tc.wantLen)
		}
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

func TestParseSystemPrincipals(t *testing.T) {
	const router = "spiffe://cluster.local/ns/ate-system/sa/atenet-router"
	got, err := parseSystemPrincipals(SystemPrincipals{
		ControllerSPIFFEID:       "spiffe://cluster.local/ns/ate-system/sa/ate-controller",
		EgressGatewaySPIFFEIDs:   []string{router, "spiffe://cluster.local/ns/plugins/sa/token-exchange"},
		IngressRouterSPIFFEID:    router,
		AteletSPIFFEID:           "spiffe://cluster.local/ns/ate-system/sa/atelet",
		APIServerSidecarSPIFFEID: "spiffe://cluster.local/ns/ate-system/sa/ate-api-server",
	})
	if err != nil {
		t.Fatalf("parseSystemPrincipals failed: %v", err)
	}
	want := map[string][]string{
		"user:spiffe%3A//cluster.local/ns/ate-system/sa/ate-controller": {RoleController},
		"user:spiffe%3A//cluster.local/ns/ate-system/sa/atenet-router":  {RoleIngressRouter, RoleEgressGateway},
		"user:spiffe%3A//cluster.local/ns/plugins/sa/token-exchange":    {RoleEgressGateway},
		"user:spiffe%3A//cluster.local/ns/ate-system/sa/atelet":         {RoleAtelet},
		"user:spiffe%3A//cluster.local/ns/ate-system/sa/ate-api-server": {RoleAPIServerSidecar},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("parseSystemPrincipals (-want +got):\n%s", diff)
	}
}

func TestNewRejectsInvalidSystemPrincipals(t *testing.T) {
	defaults := testSystemPrincipals()
	for _, tc := range []struct {
		name    string
		mutate  func(*SystemPrincipals)
		wantErr string
	}{
		{"empty controller SPIFFE ID", func(p *SystemPrincipals) { p.ControllerSPIFFEID = "" }, "invalid controller SPIFFE ID"},
		{"no egress gateway SPIFFE IDs", func(p *SystemPrincipals) { p.EgressGatewaySPIFFEIDs = nil }, "no egress_gateway SPIFFE ID"},
		{"empty egress gateway SPIFFE ID in list", func(p *SystemPrincipals) {
			p.EgressGatewaySPIFFEIDs = []string{defaults.EgressGatewaySPIFFEIDs[0], ""}
		}, "invalid egress_gateway SPIFFE ID"},
		{"empty ingress router SPIFFE ID", func(p *SystemPrincipals) { p.IngressRouterSPIFFEID = "" }, "invalid ingress_router SPIFFE ID"},
		{"empty atelet SPIFFE ID", func(p *SystemPrincipals) { p.AteletSPIFFEID = "" }, "invalid atelet SPIFFE ID"},
		{"empty api server sidecar SPIFFE ID", func(p *SystemPrincipals) { p.APIServerSidecarSPIFFEID = "" }, "invalid api_server_sidecar SPIFFE ID"},
		{"bare ServiceAccount name", func(p *SystemPrincipals) { p.ControllerSPIFFEID = "ate-controller" }, "invalid controller SPIFFE ID"},
		{"non-spiffe URI", func(p *SystemPrincipals) {
			p.EgressGatewaySPIFFEIDs = []string{"https://cluster.local/ns/ate-system/sa/atenet-egress"}
		}, "invalid egress_gateway SPIFFE ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := defaults
			tc.mutate(&p)
			// Principals are validated before the store is touched, so no
			// database is needed.
			if _, _, err := New(context.Background(), nil, nil, nil, p); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("New(%+v) error = %v, want substring %q", p, err, tc.wantErr)
			}
		})
	}
}
