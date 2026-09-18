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
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
	"github.com/agent-substrate/substrate/internal/testcert"
)

func TestFetch(t *testing.T) {
	const traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Errorf("upstream method = %s, want GET", r.Method)
		}
		if got := r.Header.Get("traceparent"); got != traceparent {
			t.Errorf("upstream traceparent = %q, want %q", got, traceparent)
		}
		return &http.Response{
			StatusCode: http.StatusTeapot,
			Body:       io.NopCloser(strings.NewReader("hello from upstream")),
			Header:     make(http.Header),
		}, nil
	})}

	payload, err := json.Marshal(fetchRequest{URL: "https://allowed.example/"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload)))
	request.Header.Set("traceparent", traceparent)
	newHandler(client).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTeapot)
	}
	var got fetchResponse
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if got.StatusCode != http.StatusTeapot || got.Body != "hello from upstream" {
		t.Errorf("response = %+v", got)
	}
}

func TestInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "method", method: http.MethodGet, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "missing hostname", method: http.MethodPost, body: `{"url":"https:///path"}`, status: http.StatusBadRequest},
		{name: "unsupported scheme", method: http.MethodPost, body: `{"url":"file:///etc/passwd"}`, status: http.StatusBadRequest},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/", strings.NewReader(test.body))
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

func TestOutboundFailure(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("blocked")
	})}
	handler := newHandler(client)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"url":"https://example.com/"}`))

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

func TestFetchWithRequestScopedTLSAndHTTP1(t *testing.T) {
	server, rootCA := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Proto != "HTTP/1.1" {
			t.Errorf("request protocol = %s, want HTTP/1.1", r.Proto)
		}
		_, _ = io.WriteString(w, "trusted")
	}))

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{
		URL:    server.URL,
		RootCA: rootCA,
		HTTP1:  true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got fetchResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusOK || got.Body != "trusted" || got.Protocol != "HTTP/1.1" || !got.TLS {
		t.Errorf("response = %+v, want successful verified HTTP/1.1 TLS response", got)
	}
}

func TestFetchTLSVerificationFailure(t *testing.T) {
	server, _ := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "should not arrive")
	}))

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{URL: server.URL})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	var got fetchResponse
	decodeRecorder(t, recorder, &got)
	if got.Error == "" || got.Body != "" || got.StatusCode != 0 {
		t.Errorf("response = %+v, want TLS failure without upstream response", got)
	}
}

func TestFetchTLSWrongRootFails(t *testing.T) {
	server, _ := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "should not arrive")
	}))
	_, wrongRoot := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "other server")
	}))

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{URL: server.URL, RootCA: wrongRoot})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

func TestFetchDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected = true
		_, _ = io.WriteString(w, "redirected")
	}))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(source.Close)

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{URL: source.URL, HTTP1: true})
	if recorder.Code != http.StatusFound {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusFound, recorder.Body.String())
	}
	if redirected {
		t.Fatal("redirect target received a request")
	}
	var got fetchResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusFound || got.Protocol == "" {
		t.Errorf("response = %+v, want observed redirect response", got)
	}
}

func TestWebSocket(t *testing.T) {
	server := startWebSocketServer(t, false)
	messages := []string{"one", "two", "three"}
	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server), Messages: messages})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusSwitchingProtocols || got.Protocol != "HTTP/1.1" || got.TLS || got.Error != "" {
		t.Errorf("response = %+v, want successful plaintext handshake", got)
	}
	if len(got.Messages) != len(messages) {
		t.Fatalf("messages = %+v, want %d observations", got.Messages, len(messages))
	}
	for i, message := range got.Messages {
		if message.Type != websocket.TextMessage || message.Message != messages[i] {
			t.Errorf("messages[%d] = %+v, want text %q", i, message, messages[i])
		}
	}
}

func TestWebSocketTLSAndVerificationFailure(t *testing.T) {
	server, rootCA := startIPTLSServer(t, webSocketEchoHandler(false))
	messages := []string{"tls-one", "tls-two", "tls-three"}
	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server.URL), RootCA: rootCA, Messages: messages})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusSwitchingProtocols || got.Protocol != "HTTP/1.1" || !got.TLS || len(got.Messages) != len(messages) {
		t.Errorf("response = %+v, want verified TLS observations", got)
	}

	recorder = postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server.URL), Messages: messages})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("untrusted status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

func TestWebSocketRejectsNonUpgrade(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "not websocket")
	}))
	t.Cleanup(server.Close)

	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server.URL), Messages: []string{"one", "two", "three"}})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusOK || got.Protocol != "HTTP/1.1" || got.TLS || got.Error == "" {
		t.Errorf("response = %+v, want preserved non-upgrade response and stage error", got)
	}
}

func TestWebSocketPreservesPartialObservationsOnBadResponse(t *testing.T) {
	server := startWebSocketServer(t, true)
	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server), Messages: []string{"one", "two", "three"}})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if len(got.Messages) != 1 || got.Messages[0].Message != "one" || got.Messages[0].Type != websocket.TextMessage || got.Error == "" {
		t.Errorf("response = %+v, want first observation and read failure", got)
	}
}

func postHandler(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decodeRecorder(t *testing.T, recorder *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.NewDecoder(recorder.Body).Decode(dst); err != nil {
		t.Fatalf("decoding response (HTTP %d): %v", recorder.Code, err)
	}
}

func startWebSocketServer(t *testing.T, badSecondResponse bool) string {
	t.Helper()
	server := httptest.NewServer(webSocketEchoHandler(badSecondResponse))
	t.Cleanup(server.Close)
	return server.URL
}

func webSocketEchoHandler(badSecondResponse bool) http.Handler {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for index := 0; ; index++ {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if badSecondResponse && index == 1 {
				_ = conn.Close()
				return
			}
			if err := conn.WriteMessage(messageType, message); err != nil {
				return
			}
		}
	})
}

func startIPTLSServer(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	material := testcert.NewServerTLS(t, net.ParseIP("127.0.0.1"))
	cert, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
	if err != nil {
		t.Fatalf("loading server certificate: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, string(material.RootCA)
}

func toWebSocketURL(raw string) string {
	if strings.HasPrefix(raw, "https://") {
		return strings.Replace(raw, "https://", "wss://", 1)
	}
	return strings.Replace(raw, "http://", "ws://", 1)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// The gRPC endpoint below is what an e2e reads as evidence that gRPC crossed
// the egress tunnel, so these tests pin its answers against a loopback server,
// where there is no tunnel to blame.

// echoServer is the in-process stand-in for the egress e2e gRPC origin
// (internal/e2e/fixtures/testserver, its grpc subcommand).
type echoServer struct {
	grpcechopb.UnimplementedEchoServer
}

func (echoServer) Echo(_ context.Context, req *grpcechopb.EchoRequest) (*grpcechopb.EchoResponse, error) {
	return &grpcechopb.EchoResponse{Message: req.GetMessage()}, nil
}

func (echoServer) EchoStream(req *grpcechopb.EchoStreamRequest, stream grpc.ServerStreamingServer[grpcechopb.EchoResponse]) error {
	for i := range req.GetCount() {
		if err := stream.Send(&grpcechopb.EchoResponse{Message: req.GetMessage(), Index: i}); err != nil {
			return err
		}
	}
	return nil
}

// EchoBidi answers each request as it arrives, like the fixture: the handler
// under test sends one message at a time and blocks on its response, so a
// stand-in that drained the request direction first would deadlock.
func (echoServer) EchoBidi(stream grpc.BidiStreamingServer[grpcechopb.EchoRequest, grpcechopb.EchoResponse]) error {
	for index := int32(0); ; index++ {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&grpcechopb.EchoResponse{Message: req.GetMessage(), Index: index}); err != nil {
			return err
		}
	}
}

// startEchoServer serves Echo on loopback and returns its host:port.
func startEchoServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := grpc.NewServer()
	grpcechopb.RegisterEchoServer(server, echoServer{})
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("serving: %v", err)
		}
	}()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

// postGRPC drives the /grpc endpoint and returns the recorder and the decoded
// body, which is what the e2e asserts on.
func postGRPC(t *testing.T, body string) (*httptest.ResponseRecorder, grpcResponse) {
	t.Helper()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/grpc", strings.NewReader(body))
	newHandler(http.DefaultClient).ServeHTTP(recorder, request)

	var decoded grpcResponse
	if err := json.NewDecoder(recorder.Body).Decode(&decoded); err != nil {
		t.Fatalf("decoding response (HTTP %d): %v", recorder.Code, err)
	}
	return recorder, decoded
}

func TestGRPCUnary(t *testing.T) {
	target := startEchoServer(t)

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"hello"}`, target))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if got.Message != "hello" {
		t.Errorf("message = %q, want %q", got.Message, "hello")
	}
	if got.Code != codes.OK.String() {
		t.Errorf("code = %q, want %q", got.Code, codes.OK.String())
	}
	// A request that did not ask for a stream must not report one, so the e2e
	// cannot pass its streaming assertion against leftover unary state.
	if got.Stream != nil {
		t.Errorf("stream = %+v, want none for a request with no streamCount", got.Stream)
	}
}

func TestGRPCStream(t *testing.T) {
	target := startEchoServer(t)
	const count = 3

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"streamed","streamCount":%d}`, target, count))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.Stream) != count {
		t.Fatalf("stream has %d messages, want %d: %+v", len(got.Stream), count, got.Stream)
	}
	for i, message := range got.Stream {
		if message.Message != "streamed" {
			t.Errorf("stream[%d].message = %q, want %q", i, message.Message, "streamed")
		}
		if int(message.Index) != i {
			t.Errorf("stream[%d].index = %d, want %d", i, message.Index, i)
		}
	}
}

func TestGRPCBidi(t *testing.T) {
	target := startEchoServer(t)
	const count = 3

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"duplex","bidiCount":%d}`, target, count))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.Bidi) != count {
		t.Fatalf("bidi has %d messages, want %d: %+v", len(got.Bidi), count, got.Bidi)
	}
	// The handler numbers its own messages, so the echoed text pins each
	// response to the request it answered rather than to any response.
	for i, message := range got.Bidi {
		want := fmt.Sprintf("duplex-%d", i)
		if message.Message != want {
			t.Errorf("bidi[%d].message = %q, want %q", i, message.Message, want)
		}
		if int(message.Index) != i {
			t.Errorf("bidi[%d].index = %d, want %d", i, message.Index, i)
		}
	}
	// Asking only for a bidi stream must not report a server-stream, so the
	// e2e's two assertions cannot pass on each other's data.
	if got.Stream != nil {
		t.Errorf("stream = %+v, want none for a request with no streamCount", got.Stream)
	}
}

// A failed RPC must still report its gRPC code. That code is the only part of
// the answer that proves trailers arrived, so collapsing it into the HTTP
// status would erase the thing the e2e is looking for.
func TestGRPCFailureReportsStatusCode(t *testing.T) {
	// A port with nothing behind it: the dial fails, and grpc-go reports that
	// as Unavailable.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	target := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("closing loopback listener: %v", err)
	}

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"hello"}`, target))

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if got.Code != codes.Unavailable.String() {
		t.Errorf("code = %q, want %q; error = %s", got.Code, codes.Unavailable.String(), got.Error)
	}
}

func TestGRPCInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "method", method: http.MethodGet, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "missing target", method: http.MethodPost, body: `{"message":"hi"}`, status: http.StatusBadRequest},
		{name: "no port", method: http.MethodPost, body: `{"target":"grpcecho"}`, status: http.StatusBadRequest},
		{name: "no host", method: http.MethodPost, body: `{"target":":50051"}`, status: http.StatusBadRequest},
		{name: "non-numeric port", method: http.MethodPost, body: `{"target":"grpcecho:grpc"}`, status: http.StatusBadRequest},
		{name: "URL not host:port", method: http.MethodPost, body: `{"target":"http://grpcecho:50051"}`, status: http.StatusBadRequest},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/grpc", strings.NewReader(test.body))
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}
