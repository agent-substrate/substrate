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
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebsocketDefaultRespondsToPingWithPong(t *testing.T) {
	server := startOriginServer(t, newWebsocketHandler(false), "", "")
	dialer := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	conn, response, err := dialer.Dial(server.wsURL+"/ws", nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("websocket handshake status = %d, want %d", response.StatusCode, http.StatusSwitchingProtocols)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set websocket read deadline: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set websocket write deadline: %v", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte("PING")); err != nil {
		t.Fatalf("write PING: %v", err)
	}
	messageType, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read PONG: %v", err)
	}
	if messageType != websocket.TextMessage || string(message) != "PONG" {
		t.Errorf("PING response = type %d, %q; want text PONG", messageType, message)
	}
}

func TestWebsocketEchoPreservesMessageTypeAndPayload(t *testing.T) {
	cert := writeOriginCertificate(t)
	server := startOriginServer(t, newWebsocketHandler(true), cert.certFile, cert.keyFile)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert.caPEM) {
		t.Fatal("adding fixture CA to trust pool")
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: 2 * time.Second,
		TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			ServerName: "127.0.0.1",
		},
	}
	conn, response, err := dialer.Dial(server.wssURL+"/ws", nil)
	if err != nil {
		t.Fatalf("dial secure websocket: %v", err)
	}
	defer conn.Close()
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("secure websocket handshake status = %d, want %d", response.StatusCode, http.StatusSwitchingProtocols)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set secure websocket read deadline: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set secure websocket write deadline: %v", err)
	}

	messages := []struct {
		messageType int
		payload     []byte
	}{
		{websocket.TextMessage, []byte("first message")},
		{websocket.BinaryMessage, []byte{0, 1, 2, 255}},
		{websocket.TextMessage, []byte("PING")},
	}
	for i, want := range messages {
		if err := conn.WriteMessage(want.messageType, want.payload); err != nil {
			t.Fatalf("write message %d: %v", i, err)
		}
		gotType, gotPayload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read echo %d: %v", i, err)
		}
		if gotType != want.messageType {
			t.Errorf("echo %d message type = %d, want %d", i, gotType, want.messageType)
		}
		if string(gotPayload) != string(want.payload) {
			t.Errorf("echo %d payload = %q, want %q", i, gotPayload, want.payload)
		}
	}
}
