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
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
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
// whether the actor must have verified the origin certificate.
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
	// Check reply count before indexing; each reply must be a text message
	// (websocket.TextMessage) with the matching input, in order.
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
