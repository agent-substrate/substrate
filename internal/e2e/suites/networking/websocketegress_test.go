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

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/gorilla/websocket"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// egressWebSocketResponse mirrors observations returned by the actor. The
// outer HTTP 200 is already checked before these protocol assertions run.
type egressWebSocketResponse struct {
	StatusCode int                      `json:"statusCode"`
	Protocol   string                   `json:"protocol"`
	TLS        bool                     `json:"tls"`
	Messages   []egressWebSocketMessage `json:"messages"`
	Error      string                   `json:"error"`
}

type egressWebSocketMessage struct {
	Type    int    `json:"type"`
	Message string `json:"message"`
}

// assertWebSocketExchange checks the outbound handshake and every reply on
// the same connection. want lists the sent messages in order; wantTLS states
// whether the actor must have verified the TLS peer certificate.
func assertWebSocketExchange(t *testing.T, got egressWebSocketResponse, want []string, wantTLS bool) {
	t.Helper()
	if got.Error != "" {
		t.Fatalf("WebSocket exchange error: %s", got.Error)
	}
	if got.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("WebSocket handshake status = %d, want %d", got.StatusCode, http.StatusSwitchingProtocols)
	}
	if got.Protocol != "HTTP/1.1" {
		t.Errorf("WebSocket handshake protocol = %q, want HTTP/1.1", got.Protocol)
	}
	if got.TLS != wantTLS {
		t.Errorf("WebSocket TLS = %t, want %t", got.TLS, wantTLS)
	}
	if len(got.Messages) != len(want) {
		t.Fatalf("WebSocket messages = %+v, want %d replies", got.Messages, len(want))
	}
	for i, wantMessage := range want {
		gotMessage := got.Messages[i]
		if gotMessage.Type != websocket.TextMessage || gotMessage.Message != wantMessage {
			t.Errorf("WebSocket messages[%d] = %+v, want text %q", i, gotMessage, wantMessage)
		}
	}
}

func TestActorEgressWebSocket(t *testing.T) {
	ctx := t.Context()
	target, address, _ := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "ws-origin", ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"websocket", "--echo"}, Port: 80, TargetPort: 8080, HealthPath: "/readyz",
	}, false)
	actorName, router, actorRef := prepareProtocolActor(t, ctx, "ws")
	messages := []string{"ws-first", "ws-second", "ws-third"}
	since := metav1.NewTime(time.Now().Add(-time.Minute))
	raw := postEgressOnce(t, ctx, router, actorRef, "/websocket", map[string]any{"url": "ws://" + address + "/ws", "messages": messages})
	var got egressWebSocketResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding WebSocket observation: %v", err)
	}
	assertProtocolGateway(t, ctx, since, actorName, target.Address())
	assertWebSocketExchange(t, got, messages, false)
}

func TestActorEgressSecureWebSocket(t *testing.T) {
	ctx := t.Context()
	target, address, rootCA := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "wss-origin", ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"websocket", "--echo"}, Port: 443, TargetPort: 8443, HealthPath: "/readyz",
	}, true)
	actorName, router, actorRef := prepareProtocolActor(t, ctx, "wss")
	messages := []string{"wss-first", "wss-second", "wss-third"}
	since := metav1.NewTime(time.Now().Add(-time.Minute))
	raw := postEgressOnce(t, ctx, router, actorRef, "/websocket", map[string]any{
		"url": "wss://" + address + "/ws", "rootCA": rootCA, "messages": messages,
	})
	var got egressWebSocketResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding secure WebSocket observation: %v", err)
	}
	assertProtocolGateway(t, ctx, since, actorName, target.Address())
	assertWebSocketExchange(t, got, messages, true)
}
