// Copyright 2026 The Agent Substrate Authors
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
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
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

// websocketTextMessage is the RFC 6455 text frame opcode the actor reports.
const websocketTextMessage = 1

func TestActorEgressWebSocket(t *testing.T) {
	ctx := t.Context()
	_, address, _ := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "ws-origin", ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"websocket", "--echo"}, Port: 80, TargetPort: 8080, HealthPath: "/readyz",
	}, false)
	_, router, actorRef := prepareProtocolActor(t, ctx, "ws")
	assertEgressWebSocket(t, ctx, router, actorRef, "ws://"+address+"/ws", "ws-message", false)
}

func TestActorEgressSecureWebSocket(t *testing.T) {
	ctx := t.Context()
	_, address, _ := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "wss-origin", ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"websocket", "--echo"}, Port: 80, TargetPort: 8080, TLSPort: 443, HealthPath: "/readyz",
	}, true)
	_, router, actorRef := prepareProtocolActor(t, ctx, "wss")
	assertEgressWebSocket(t, ctx, router, actorRef, "wss://"+address+"/ws", "wss-message", true)
}

// assertEgressWebSocket opens a WebSocket from the actor to url and checks
// that the origin echoes message. The agentgateway dataplane does not carry
// WebSocket: there the handshake must get 403 before the origin sees a message.
func assertEgressWebSocket(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, url, message string, wantTLS bool) {
	t.Helper()
	input := map[string]any{"url": url, "messages": []string{message}}
	wantStatus := http.StatusOK
	if e2e.IsAgentGateway() {
		wantStatus = http.StatusBadGateway
	}
	var got egressWebSocketResponse
	if err := json.Unmarshal(postEgressWithStatus(t, ctx, router, actorRef, "/websocket", input, wantStatus), &got); err != nil {
		t.Fatalf("decoding WebSocket observation: %v", err)
	}
	if e2e.IsAgentGateway() {
		if got.StatusCode != http.StatusForbidden || got.HandshakeBody != "WebSocket egress is not supported" || got.Error == "" || len(got.Messages) != 0 {
			t.Errorf("WebSocket to %s = %+v; want a handshake refused with 403 and no echoes", url, got)
		}
		return
	}
	if got.Error != "" || got.StatusCode != http.StatusSwitchingProtocols || got.TLS != wantTLS {
		t.Errorf("WebSocket to %s = %+v; want a 101 handshake with verified TLS = %v", url, got, wantTLS)
	}
	if want := []egressWebSocketMessage{{Type: websocketTextMessage, Message: message}}; !slices.Equal(got.Messages, want) {
		t.Errorf("WebSocket echoes from %s = %+v, want %+v", url, got.Messages, want)
	}
}
