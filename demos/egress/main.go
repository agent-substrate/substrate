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

// Command egress is a small HTTP service for demonstrating per-Actor egress
// policy. It accepts a URL, fetches it, and returns the upstream response, and
// on a second endpoint it makes gRPC calls and returns what came back.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
)

const (
	listenAddress             = ":80"
	maxRequestBody            = 64 << 10
	maxResponseBody           = 1 << 20
	requestTimeout            = 15 * time.Second
	maxWebSocketMessages      = 16
	maxWebSocketMessageSize   = 64 << 10
	maxWebSocketHandshakeBody = 4 << 10
	muxRequestCount           = 250
	muxBodySize               = 64
)

type fetchRequest struct {
	URL    string `json:"url"`
	RootCA string `json:"rootCA,omitempty"`
	HTTP1  bool   `json:"http1,omitempty"`
}

type fetchResponse struct {
	StatusCode int    `json:"statusCode,omitempty"`
	Body       string `json:"body,omitempty"`
	ServerCert string `json:"serverCert,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	TLS        bool   `json:"tls"`
	Error      string `json:"error,omitempty"`
}

type websocketRequest struct {
	URL      string   `json:"url"`
	RootCA   string   `json:"rootCA,omitempty"`
	Messages []string `json:"messages"`
}

type websocketMessage struct {
	Type    int    `json:"type"`
	Message string `json:"message"`
}

type websocketResponse struct {
	StatusCode    int                `json:"statusCode,omitempty"`
	Protocol      string             `json:"protocol,omitempty"`
	HandshakeBody string             `json:"handshakeBody,omitempty"`
	TLS           bool               `json:"tls"`
	Messages      []websocketMessage `json:"messages"`
	Error         string             `json:"error,omitempty"`
}

// grpcRequest asks for one unary Echo against target, and additionally for a
// server-stream or a bidirectional stream when the matching count is positive.
type grpcRequest struct {
	// Target is the gRPC server to dial, as host:port. Cleartext HTTP/2: the
	// point of the demo is that the actor speaks plainly and the egress path
	// carries it, so there is nothing here to configure TLS with.
	Target      string `json:"target"`
	Message     string `json:"message"`
	StreamCount int32  `json:"streamCount,omitempty"`
	// BidiCount is how many messages to send over a bidirectional stream,
	// one at a time, each awaiting its response before the next goes out.
	BidiCount int32 `json:"bidiCount,omitempty"`
}

type grpcResponse struct {
	// Message is what the unary Echo returned.
	Message string `json:"message"`
	// Stream is what EchoStream returned, in the order it arrived. Absent when
	// the request did not ask for a stream.
	Stream []streamedMessage `json:"stream,omitempty"`
	// Bidi is what EchoBidi returned, in the order it arrived. Absent when the
	// request did not ask for one.
	Bidi []streamedMessage `json:"bidi,omitempty"`
	// Code is the gRPC status of the last RPC attempted, as a string. It is the
	// one field an HTTP status cannot stand in for: a gRPC status travels in
	// trailers, after the response body, so a path that drops trailers or
	// downgrades the connection to HTTP/1.1 cannot produce this at all.
	Code  string `json:"code,omitempty"`
	Error string `json:"error,omitempty"`
}

type streamedMessage struct {
	Message string `json:"message"`
	Index   int32  `json:"index"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	client := &http.Client{Timeout: requestTimeout}
	slog.Info("starting egress demo", "address", listenAddress)
	if err := http.ListenAndServe(listenAddress, newHandler(client)); err != nil {
		slog.Error("egress demo stopped", "error", err)
		os.Exit(1)
	}
}

func newHandler(client *http.Client) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, fetchResponse{Error: "method must be POST"})
			return
		}

		var input fetchRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err := decoder.Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
			return
		}
		if err := validateURL(input.URL); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: err.Error()})
			return
		}

		outbound, err := http.NewRequestWithContext(r.Context(), http.MethodGet, input.URL, nil)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid URL: %v", err)})
			return
		}
		if traceparent := r.Header.Get("traceparent"); traceparent != "" {
			outbound.Header.Set("traceparent", traceparent)
		}
		requestClient, cleanup, err := fetchClient(client, input)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("configuring request: %v", err)})
			return
		}
		defer cleanup()
		response, err := requestClient.Do(outbound)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}
		defer response.Body.Close()

		body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody))
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("reading response: %v", err)})
			return
		}
		var serverCert string
		verifiedTLS := response.TLS != nil && len(response.TLS.VerifiedChains) > 0
		if response.TLS != nil && len(response.TLS.PeerCertificates) > 0 {
			serverCert = response.TLS.PeerCertificates[0].Issuer.String()
		}
		writeJSON(w, response.StatusCode, fetchResponse{
			StatusCode: response.StatusCode,
			Body:       string(body),
			ServerCert: serverCert,
			Protocol:   response.Proto,
			TLS:        verifiedTLS,
		})
	})
	mux.HandleFunc("/mux", handleMux)
	mux.HandleFunc("/websocket", handleWebSocket)
	mux.HandleFunc("/grpc", handleGRPC)
	return mux
}

// This handler is for testing mutiplexing of actor `muxRequestCount` connections over H/2
// CONNECT tunnels between an atunnel and egress gateway. This handler:
// 1. opens `muxRequestCount` connections to the origin
// 2. sends POST request with headers, but not bodies on all of them.
// 3. once headers were sent, it sends bodies
// 4. waits for all 200 responses.
// Tests can then validate the expected number of H/2 connections on egress gateways.
func handleMux(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, fetchResponse{Error: "method must be POST"})
		return
	}

	var input fetchRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if err := validateURL(input.URL); err != nil {
		writeJSON(w, http.StatusBadRequest, fetchResponse{Error: err.Error()})
		return
	}
	parsed, err := url.Parse(input.URL)
	if err != nil || parsed.Scheme != "http" {
		writeJSON(w, http.StatusBadRequest, fetchResponse{Error: "URL scheme must be http"})
		return
	}

	targetAddr := parsed.Host
	if parsed.Port() == "" {
		targetAddr = net.JoinHostPort(parsed.Hostname(), "80")
	}
	requestURI := parsed.RequestURI()

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	var headersWg sync.WaitGroup
	headersWg.Add(muxRequestCount)
	allHeadersSent := make(chan struct{})
	go func() {
		headersWg.Wait()
		close(allHeadersSent)
	}()

	type muxOutcome struct {
		status int
		body   string
		err    error
	}
	outcomes := make([]muxOutcome, muxRequestCount)

	var doneWg sync.WaitGroup
	doneWg.Add(muxRequestCount)
	for i := range muxRequestCount {
		go func(idx int) {
			defer doneWg.Done()
			status, body, reqErr := sendMuxRequest(ctx, cancel, targetAddr, parsed.Host, requestURI, &headersWg, allHeadersSent)
			outcomes[idx] = muxOutcome{status: status, body: body, err: reqErr}
		}(i)
	}
	doneWg.Wait()

	for i, out := range outcomes {
		if out.err != nil && !errors.Is(out.err, context.Canceled) {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request %d failed: %v", i, out.err)})
			return
		}
		if out.err == nil && out.status != http.StatusOK {
			writeJSON(w, out.status, fetchResponse{
				StatusCode: out.status,
				Body:       out.body,
				Error:      fmt.Sprintf("request %d returned status %d, want 200", i, out.status),
			})
			return
		}
	}
	for i, out := range outcomes {
		if out.err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request %d failed: %v", i, out.err)})
			return
		}
	}

	writeJSON(w, http.StatusOK, fetchResponse{StatusCode: http.StatusOK})
}

func sendMuxRequest(ctx context.Context, cancel context.CancelFunc, targetAddr, host, requestURI string, headersWg *sync.WaitGroup, allHeadersSent <-chan struct{}) (int, string, error) {
	headerDone := false
	defer func() {
		if !headerDone {
			cancel()
			headersWg.Done()
		}
	}()

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		return 0, "", fmt.Errorf("dialing %s: %w", targetAddr, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return 0, "", fmt.Errorf("setting deadline: %w", err)
		}
	}

	reqHeader := fmt.Sprintf(
		"POST %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
		requestURI, host, muxBodySize,
	)
	if _, err := io.WriteString(conn, reqHeader); err != nil {
		return 0, "", fmt.Errorf("writing request headers: %w", err)
	}

	headerDone = true
	headersWg.Done()

	select {
	case <-allHeadersSent:
	case <-ctx.Done():
		return 0, "", ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return 0, "", err
	}

	body := bytes.Repeat([]byte("a"), muxBodySize)
	if _, err := conn.Write(body); err != nil {
		return 0, "", fmt.Errorf("writing request body: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		return 0, "", fmt.Errorf("reading response: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return 0, "", fmt.Errorf("reading response body: %w", err)
	}
	return resp.StatusCode, string(respBody), nil
}

// fetchClient constructs a per-request client only when the request asks for
// request-scoped TLS or protocol behavior. The default path retains the
// caller's client, including its transport and redirect policy.
func fetchClient(base *http.Client, input fetchRequest) (*http.Client, func(), error) {
	if input.RootCA == "" && !input.HTTP1 {
		return base, func() {}, nil
	}
	if base == nil {
		base = http.DefaultClient
	}
	// Start from the standard transport so request-scoped trust cannot inherit
	// an insecure TLS callback or proxy from an injected/base client.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if input.RootCA != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(input.RootCA)) {
			return nil, nil, errors.New("rootCA does not contain a valid certificate")
		}
		transport.TLSClientConfig.RootCAs = roots
	}
	if input.HTTP1 {
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		transport.Protocols = protocols
		transport.ForceAttemptHTTP2 = false
		transport.TLSNextProto = nil
	}
	requestClient := *base
	requestClient.Transport = transport
	requestClient.Timeout = requestTimeout
	requestClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &requestClient, transport.CloseIdleConnections, nil
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, websocketResponse{Messages: []websocketMessage{}, Error: "method must be POST"})
		return
	}

	var input websocketRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, websocketResponse{Messages: []websocketMessage{}, Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if err := validateWebSocketRequest(input); err != nil {
		writeJSON(w, http.StatusBadRequest, websocketResponse{Messages: []websocketMessage{}, Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	response, err := performWebSocket(ctx, input)
	if err != nil {
		response.Error = err.Error()
		writeJSON(w, http.StatusBadGateway, response)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func validateWebSocketRequest(input websocketRequest) error {
	parsed, err := url.Parse(input.URL)
	if err != nil {
		return fmt.Errorf("invalid WebSocket URL: %w", err)
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return errors.New("WebSocket URL scheme must be ws or wss")
	}
	if parsed.Hostname() == "" {
		return errors.New("WebSocket URL must include a hostname")
	}
	if len(input.Messages) == 0 {
		return errors.New("messages must contain at least one message")
	}
	if len(input.Messages) > maxWebSocketMessages {
		return fmt.Errorf("messages contains %d items, maximum is %d", len(input.Messages), maxWebSocketMessages)
	}
	for index, message := range input.Messages {
		if len(message) > maxWebSocketMessageSize {
			return fmt.Errorf("message %d is %d bytes, maximum is %d", index, len(message), maxWebSocketMessageSize)
		}
	}
	return nil
}

func performWebSocket(ctx context.Context, input websocketRequest) (websocketResponse, error) {
	dialer := websocket.Dialer{HandshakeTimeout: requestTimeout, Proxy: nil}
	if input.RootCA != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(input.RootCA)) {
			return websocketResponse{Messages: []websocketMessage{}}, errors.New("configuring TLS: rootCA does not contain a valid certificate")
		}
		dialer.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	response := websocketResponse{Messages: []websocketMessage{}}
	conn, handshakeResponse, err := dialer.DialContext(ctx, input.URL, nil)
	if handshakeResponse != nil {
		response.StatusCode = handshakeResponse.StatusCode
		response.Protocol = handshakeResponse.Proto
		if handshakeResponse.Body != nil {
			body, _ := io.ReadAll(io.LimitReader(handshakeResponse.Body, maxWebSocketHandshakeBody))
			_ = handshakeResponse.Body.Close()
			response.HandshakeBody = string(body)
		}
	}
	if err != nil {
		return response, fmt.Errorf("WebSocket handshake failed: %w", err)
	}
	if tlsConn, ok := conn.UnderlyingConn().(*tls.Conn); ok {
		response.TLS = tlsConn.ConnectionState().VerifiedChains != nil
	}
	defer conn.Close()
	conn.SetReadLimit(maxWebSocketMessageSize)

	stopCancellation := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopCancellation:
		}
	}()
	defer close(stopCancellation)

	for index, message := range input.Messages {
		if err := conn.SetWriteDeadline(time.Now().Add(requestTimeout)); err != nil {
			return response, fmt.Errorf("setting write deadline for message %d: %w", index, err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			return response, fmt.Errorf("writing message %d: %w", index, err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(requestTimeout)); err != nil {
			return response, fmt.Errorf("setting read deadline for message %d: %w", index, err)
		}
		messageType, received, err := conn.ReadMessage()
		if err != nil {
			return response, fmt.Errorf("reading response to message %d: %w", index, err)
		}
		response.Messages = append(response.Messages, websocketMessage{Type: messageType, Message: string(received)})
	}
	return response, nil
}

// handleGRPC dials the requested target and echoes back what the RPCs returned.
// It exists so an e2e can assert that gRPC survives the egress path: HTTP/2
// framing end to end, and a status delivered in trailers.
func handleGRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, grpcResponse{Error: "method must be POST"})
		return
	}

	var input grpcRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, grpcResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if err := validateTarget(input.Target); err != nil {
		writeJSON(w, http.StatusBadRequest, grpcResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	// Dialed per request and closed with it. The actor this runs inside is
	// checkpointed and restored, and an HTTP/2 connection opened before a
	// snapshot does not survive one: the peer is long gone by the time the
	// actor resumes, and every RPC on it would fail in a way that looks like a
	// broken gateway.
	conn, err := grpc.NewClient(input.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, grpcResponse{Error: fmt.Sprintf("dialing %s: %v", input.Target, err)})
		return
	}
	defer conn.Close()

	client := grpcechopb.NewEchoClient(conn)
	echoed, err := client.Echo(ctx, &grpcechopb.EchoRequest{Message: input.Message})
	if err != nil {
		writeGRPCFailure(w, "Echo", err)
		return
	}

	response := grpcResponse{Message: echoed.GetMessage(), Code: codes.OK.String()}
	if input.StreamCount > 0 {
		response.Stream, err = echoStream(ctx, client, input)
		if err != nil {
			writeGRPCFailure(w, "EchoStream", err)
			return
		}
	}
	if input.BidiCount > 0 {
		response.Bidi, err = echoBidi(ctx, client, input)
		if err != nil {
			writeGRPCFailure(w, "EchoBidi", err)
			return
		}
	}
	writeJSON(w, http.StatusOK, response)
}

// echoStream drains a server-stream into the response's Stream field.
func echoStream(ctx context.Context, client grpcechopb.EchoClient, input grpcRequest) ([]streamedMessage, error) {
	stream, err := client.EchoStream(ctx, &grpcechopb.EchoStreamRequest{Message: input.Message, Count: input.StreamCount})
	if err != nil {
		return nil, err
	}
	var out []streamedMessage
	for {
		received, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, streamedMessage{Message: received.GetMessage(), Index: received.GetIndex()})
	}
}

// echoBidi runs a bidirectional stream, sending BidiCount messages one at a
// time and waiting for each response before sending the next. That ordering is
// the whole point: sending everything and then reading it back would succeed
// over a path that carries one direction at a time, which is precisely the
// failure this endpoint exists to catch.
func echoBidi(ctx context.Context, client grpcechopb.EchoClient, input grpcRequest) ([]streamedMessage, error) {
	stream, err := client.EchoBidi(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]streamedMessage, 0, input.BidiCount)
	for i := range input.BidiCount {
		// A distinct message per iteration, so a path that replays or holds on
		// to a buffered frame shows up as a mismatch rather than as a pass.
		message := fmt.Sprintf("%s-%d", input.Message, i)
		if err := stream.Send(&grpcechopb.EchoRequest{Message: message}); err != nil {
			// grpc-go reports a broken stream from Send as io.EOF and puts the
			// real status on Recv, which is the code worth reporting.
			if _, recvErr := stream.Recv(); recvErr != nil {
				return nil, recvErr
			}
			return nil, err
		}
		received, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		out = append(out, streamedMessage{Message: received.GetMessage(), Index: received.GetIndex()})
	}

	// Half-close the request direction while the response direction is still
	// open, then drain it. A path that reads a one-directional END_STREAM as a
	// teardown fails here and nowhere else.
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("server sent more responses than the %d messages requested", input.BidiCount)
		}
		return nil, err
	}
	return out, nil
}

// writeGRPCFailure reports a failed RPC. The HTTP status is 502 so that a
// caller polling through the ingress router keeps retrying -- the origin may
// simply not be up yet -- while the gRPC code travels in the body, where an
// HTTP status cannot flatten it into a generic "bad gateway".
func writeGRPCFailure(w http.ResponseWriter, rpc string, err error) {
	writeJSON(w, http.StatusBadGateway, grpcResponse{
		Code:  grpcstatus.Code(err).String(),
		Error: fmt.Sprintf("%s failed: %v", rpc, err),
	})
}

func validateURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL scheme must be http or https")
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("URL must include a hostname")
	}
	return nil
}

// validateTarget checks that raw is the host:port a gRPC dial needs. Unlike a
// URL there is no scheme to reject, so a caller that passes one -- or passes a
// bare hostname and lets the dial default the port -- finds out here rather
// than in a connection error from somewhere along the egress path.
func validateTarget(raw string) error {
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return fmt.Errorf("target must be host:port: %w", err)
	}
	if host == "" {
		return fmt.Errorf("target must include a host")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("target port must be a number between 1 and 65535, got %q", port)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, statusCode int, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(response)
}
