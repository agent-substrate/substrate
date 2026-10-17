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

	"go.ate.dev/substrate/internal/e2e"
)

// egressWebSocketResponse mirrors observations returned by the actor. The
// outer HTTP status is already checked before these protocol assertions run.
type egressWebSocketResponse struct {
	StatusCode    int                      `json:"statusCode"`
	Protocol      string                   `json:"protocol"`
	HandshakeBody string                   `json:"handshakeBody"`
	TLS           bool                     `json:"tls"`
	Messages      []egressWebSocketMessage `json:"messages"`
	Error         string                   `json:"error"`
}

type egressWebSocketMessage struct {
	Type    int    `json:"type"`
	Message string `json:"message"`
}

func TestActorEgressWebSocket(t *testing.T) {
	ctx := t.Context()
	_, address, _ := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "ws-origin", ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"websocket", "--echo"}, Port: 80, TargetPort: 8080, HealthPath: "/readyz",
	}, false)
	_, router, actorRef := prepareProtocolActor(t, ctx, "ws")
	raw := postEgressWithStatus(t, ctx, router, actorRef, "/websocket", map[string]any{
		"url": "ws://" + address + "/ws", "messages": []string{"ws-message"},
	}, http.StatusBadGateway)
	var got egressWebSocketResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding WebSocket observation: %v", err)
	}
	requireEgressWebSocketDenied(t, "WebSocket", got)
}

func TestActorEgressSecureWebSocket(t *testing.T) {
	ctx := t.Context()
	_, address, _ := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "wss-origin", ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"websocket", "--echo"}, Port: 80, TargetPort: 8080, TLSPort: 443, HealthPath: "/readyz",
	}, true)
	_, router, actorRef := prepareProtocolActor(t, ctx, "wss")
	raw := postEgressWithStatus(t, ctx, router, actorRef, "/websocket", map[string]any{
		"url": "wss://" + address + "/ws", "messages": []string{"wss-message"},
	}, http.StatusBadGateway)
	var got egressWebSocketResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding secure WebSocket observation: %v", err)
	}
	requireEgressWebSocketDenied(t, "secure WebSocket", got)
}

// requireEgressWebSocketDenied checks that the dataplane refused the handshake
// and that nothing was echoed.
func requireEgressWebSocketDenied(t *testing.T, name string, got egressWebSocketResponse) {
	t.Helper()
	if !e2e.CurrentAtenetDataplane().IsWebSocketEgressDenied(got.StatusCode, got.HandshakeBody) {
		t.Errorf("%s handshake = %d %q, want a WebSocket egress denial", name, got.StatusCode, got.HandshakeBody)
	}
	if got.Error == "" {
		t.Errorf("%s handshake error is empty", name)
	}
	if len(got.Messages) != 0 {
		t.Errorf("%s messages = %+v, want no echoes", name, got.Messages)
	}
}
