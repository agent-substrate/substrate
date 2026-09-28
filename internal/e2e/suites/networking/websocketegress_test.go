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
	"os"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e"
)

// egressWebSocketResponse mirrors observations returned by the actor. The
// outer HTTP status is already checked before these protocol assertions run.
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

func TestActorEgressWebSocket(t *testing.T) {
	ctx := t.Context()
	_, address, _ := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "ws-origin", ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
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
	if got.StatusCode != http.StatusForbidden {
		t.Errorf("WebSocket handshake status = %d, want %d", got.StatusCode, http.StatusForbidden)
	}
	if got.Error == "" {
		t.Error("WebSocket handshake error is empty")
	}
	if len(got.Messages) != 0 {
		t.Errorf("WebSocket messages = %+v, want no echoes", got.Messages)
	}
}

func TestActorEgressSecureWebSocket(t *testing.T) {
	if os.Getenv("E2E_EGRESS_MITM") == "" {
		t.Skip("requires MITM egress to inspect the WebSocket handshake inside TLS")
	}
	ctx := t.Context()
	_, address, rootCA := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "wss-origin", ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"websocket", "--echo"}, Port: 443, TargetPort: 8443, HealthPath: "/readyz",
	}, true)
	_, router, actorRef := prepareProtocolActor(t, ctx, "wss")
	raw := postEgressWithStatus(t, ctx, router, actorRef, "/websocket", map[string]any{
		"url": "wss://" + address + "/ws", "rootCA": rootCA, "messages": []string{"wss-message"},
	}, http.StatusBadGateway)
	var got egressWebSocketResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding secure WebSocket observation: %v", err)
	}
	if got.StatusCode != http.StatusForbidden {
		t.Errorf("secure WebSocket handshake status = %d, want %d", got.StatusCode, http.StatusForbidden)
	}
	if got.Error == "" {
		t.Error("secure WebSocket handshake error is empty")
	}
	if len(got.Messages) != 0 {
		t.Errorf("secure WebSocket messages = %+v, want no echoes", got.Messages)
	}
}
