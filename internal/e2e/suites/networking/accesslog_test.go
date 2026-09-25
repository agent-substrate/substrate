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

package networking

import "testing"

func TestAccessLogField(t *testing.T) {
	line := `http.status=101 ate.actor.name=ws-1 substrate.connect.authority="10.96.0.7:80" ate.atespace=networking-e2e`
	for _, test := range []struct {
		key, want string
	}{
		{"http.status", "101"},
		{"ate.actor.name", "ws-1"},
		{"substrate.connect.authority", "10.96.0.7:80"},
	} {
		if got, ok := accessLogField(line, test.key); !ok || got != test.want {
			t.Errorf("accessLogField(%q, %q) = (%q, %t), want (%q, true)", line, test.key, got, ok, test.want)
		}
	}
	if got, ok := accessLogField(`other.http.status=503 http.status=101`, "http.status"); !ok || got != "101" {
		t.Errorf("accessLogField must match a complete key, got (%q, %t)", got, ok)
	}
}

func TestProtocolGatewayAccessLogMatches(t *testing.T) {
	identity := "spiffe://substrate-actor.local/atespace/networking-e2e/actor/ws-1"
	for _, test := range []struct {
		name, container, line string
		want                  bool
	}{
		{
			name:      "agentgateway websocket success",
			container: "agentgateway",
			line:      `http.status=101 ate.actor.name=ws-1 substrate.connect.authority="10.96.0.7:80" ate.atespace=networking-e2e`,
			want:      true,
		},
		{
			name:      "agentgateway opaque tunnel success",
			container: "agentgateway",
			line:      `CONNECT tunnel terminated target=10.96.0.7:80 actor_name=ws-1 actor_uid=uid-1 atespace=networking-e2e`,
			want:      true,
		},
		{
			name:      "agentgateway opaque tunnel wrong identity",
			container: "agentgateway",
			line:      `CONNECT tunnel terminated target=10.96.0.7:80 actor_name=ws-2 actor_uid=uid-1 atespace=networking-e2e substrate.connect.authority="10.96.0.7:80"`,
			want:      false,
		},
		{
			name:      "agentgateway opaque tunnel wrong target",
			container: "agentgateway",
			line:      `CONNECT tunnel terminated target=10.96.0.8:80 actor_name=ws-1 actor_uid=uid-1 atespace=networking-e2e substrate.connect.authority="10.96.0.7:80"`,
			want:      false,
		},
		{
			name:      "agentgateway opaque tunnel wrong atespace",
			container: "agentgateway",
			line:      `CONNECT tunnel terminated target=10.96.0.7:80 actor_name=ws-1 actor_uid=uid-1 atespace=other`,
			want:      false,
		},
		{
			name:      "agentgateway opaque tunnel error",
			container: "agentgateway",
			line:      `CONNECT tunnel terminated target=10.96.0.7:80 actor_name=ws-1 actor_uid=uid-1 atespace=networking-e2e error=denied`,
			want:      false,
		},
		{
			name:      "agentgateway wrong destination",
			container: "agentgateway",
			line:      `http.status=101 ate.actor.name=ws-1 substrate.connect.authority="10.96.0.8:80" ate.atespace=networking-e2e`,
			want:      false,
		},
		{
			name:      "agentgateway wrong actor",
			container: "agentgateway",
			line:      `http.status=101 ate.actor.name=ws-2 substrate.connect.authority="10.96.0.7:80" ate.atespace=networking-e2e`,
			want:      false,
		},
		{
			name:      "agentgateway wrong atespace",
			container: "agentgateway",
			line:      `http.status=101 ate.actor.name=ws-1 substrate.connect.authority="10.96.0.7:80" ate.atespace=other`,
			want:      false,
		},
		{
			name:      "agentgateway rejected request",
			container: "agentgateway",
			line:      `http.status=403 ate.actor.name=ws-1 substrate.connect.authority="10.96.0.7:80" ate.atespace=networking-e2e`,
			want:      false,
		},
		{
			name:      "envoy success",
			container: "envoy",
			line:      `authority=10.96.0.7:80 code=200 peer_san="` + identity + `"`,
			want:      true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := protocolGatewayAccessLogMatches(gatewayAccessLogLine{container: test.container, text: test.line}, "ws-1", identity, "10.96.0.7:80", true)
			if got != test.want {
				t.Fatalf("protocolGatewayAccessLogMatches() = %t, want %t", got, test.want)
			}
		})
	}
	valid := gatewayAccessLogLine{container: "agentgateway", text: `CONNECT tunnel terminated target=10.96.0.7:80 actor_name=ws-1 actor_uid=uid-1 atespace=networking-e2e`}
	if protocolGatewayAccessLogMatches(valid, "ws-1", identity, "10.96.0.7:80", false) {
		t.Fatal("opaque CONNECT evidence must not satisfy the MITM matcher")
	}
}
