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

package glutton

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// TestSplitGRPCServesReadyzAndGRPCOnOneListener starts the grpc-mode handler
// on a real listener and exercises both protocols against it: the readyz
// probe is a plain HTTP GET, and it must not stop gRPC from being served.
func TestSplitGRPCServesReadyzAndGRPCOnOneListener(t *testing.T) {
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	grpcSrv := grpc.NewServer()
	gluttonpb.RegisterGluttonServer(grpcSrv, svc)

	mux := http.NewServeMux()
	mux.HandleFunc(ReadyzRoute, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer(splitGRPC(grpcSrv, mux))
	go srv.Serve(lis)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := http.Get("http://" + lis.Addr().String() + ReadyzRoute)
	if err != nil {
		t.Fatalf("GET %s: %v", ReadyzRoute, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s = %d, want %d", ReadyzRoute, resp.StatusCode, http.StatusOK)
	}

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	pong, err := gluttonpb.NewGluttonClient(conn).Ping(ctx, &gluttonpb.PingRequest{Message: "hi"})
	if err != nil {
		t.Fatalf("Ping over gRPC: %v", err)
	}
	if pong.GetMessage() != "hi" {
		t.Errorf("Ping = %q, want %q", pong.GetMessage(), "hi")
	}
}

// TestSplitGRPCRoutesOnContentType pins the routing rule itself: an HTTP/2
// request is not enough to reach the gRPC server, the content type is what
// decides.
func TestSplitGRPCRoutesOnContentType(t *testing.T) {
	grpcHit := false
	handler := splitGRPC(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { grpcHit = true }),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) }),
	)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer(handler)
	go srv.Serve(lis)
	defer srv.Close()

	resp, err := http.Get("http://" + lis.Addr().String() + "/anything")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("HTTP/1.1 GET = %d, want %d (the non-gRPC handler)", resp.StatusCode, http.StatusTeapot)
	}
	if grpcHit {
		t.Error("HTTP/1.1 GET reached the gRPC handler")
	}
}

func TestHandlerRejectsUnknownMode(t *testing.T) {
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	for _, mode := range []string{ModeGRPC, ModeHTTP} {
		if _, err := Handler(mode, svc); err != nil {
			t.Errorf("Handler(%q): %v", mode, err)
		}
	}
	if _, err := Handler("quic", svc); err == nil {
		t.Error("Handler(\"quic\") succeeded, want error")
	}
}

func TestHTTPRoutes(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ts := httptest.NewServer(newMux(svc))
	defer ts.Close()

	// 1. /readyz GET -> 200 OK
	res, err := http.Get(ts.URL + ReadyzRoute)
	if err != nil {
		t.Fatalf("GET %s failed: %v", ReadyzRoute, err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("GET %s status: got %d, want 200", ReadyzRoute, res.StatusCode)
	}
	res.Body.Close()

	// 2. GET on /ping -> 405 Method Not Allowed
	res, err = http.Get(ts.URL + PingRoute)
	if err != nil {
		t.Fatalf("GET %s failed: %v", PingRoute, err)
	}
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET %s status: got %d, want 405", PingRoute, res.StatusCode)
	}
	res.Body.Close()

	// 3. POST bad body -> 400 Bad Request
	res, err = http.Post(ts.URL+PingRoute, "application/x-protobuf", bytes.NewReader([]byte("garbage")))
	if err != nil {
		t.Fatalf("POST %s garbage failed: %v", PingRoute, err)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("POST %s garbage status: got %d, want 400", PingRoute, res.StatusCode)
	}
	res.Body.Close()

	// 4. POST /ping -> 200 OK & protobuf Content-Type & ServerElapsedTrailer & echo message
	pingReqBytes, _ := proto.Marshal(&gluttonpb.PingRequest{Message: "hello"})
	res, err = http.Post(ts.URL+PingRoute, "application/x-protobuf", bytes.NewReader(pingReqBytes))
	if err != nil {
		t.Fatalf("POST %s failed: %v", PingRoute, err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST %s status: got %d, want 200", PingRoute, res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("POST %s Content-Type: got %q, want application/x-protobuf", PingRoute, ct)
	}
	if elapsed := res.Header.Get(ateinterceptors.ServerElapsedTrailer); elapsed == "" {
		t.Errorf("POST %s missing header %q", PingRoute, ateinterceptors.ServerElapsedTrailer)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	var pingResp gluttonpb.PingResponse
	if err := proto.Unmarshal(body, &pingResp); err != nil {
		t.Fatalf("unmarshal PingResponse failed: %v", err)
	}
	if pingResp.GetMessage() != "hello" {
		t.Errorf("PingResponse message: got %q, want 'hello'", pingResp.GetMessage())
	}

	// 5. POST /writedisk -> 200 OK & protobuf Content-Type
	writeReqBytes, _ := proto.Marshal(&gluttonpb.WriteDiskRequest{
		Key:       "httpfile",
		Size:      512,
		WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
	})
	res, err = http.Post(ts.URL+WriteDiskRoute, "application/x-protobuf", bytes.NewReader(writeReqBytes))
	if err != nil {
		t.Fatalf("POST %s failed: %v", WriteDiskRoute, err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST %s status: got %d, want 200", WriteDiskRoute, res.StatusCode)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	var writeResp gluttonpb.WriteDiskResponse
	if err := proto.Unmarshal(body, &writeResp); err != nil {
		t.Fatalf("unmarshal WriteDiskResponse failed: %v", err)
	}
	if writeResp.GetSize() != 512 {
		t.Errorf("WriteDiskResponse size: got %d, want 512", writeResp.GetSize())
	}

	// 6. POST /readdisk -> 200 OK & matching size & digest
	readReqBytes, _ := proto.Marshal(&gluttonpb.ReadDiskRequest{Key: "httpfile"})
	res, err = http.Post(ts.URL+ReadDiskRoute, "application/x-protobuf", bytes.NewReader(readReqBytes))
	if err != nil {
		t.Fatalf("POST %s failed: %v", ReadDiskRoute, err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST %s status: got %d, want 200", ReadDiskRoute, res.StatusCode)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	var readResp gluttonpb.ReadDiskResponse
	if err := proto.Unmarshal(body, &readResp); err != nil {
		t.Fatalf("unmarshal ReadDiskResponse failed: %v", err)
	}
	if readResp.GetSize() != 512 {
		t.Errorf("ReadDiskResponse size: got %d, want 512", readResp.GetSize())
	}
	if !bytes.Equal(readResp.GetSha256(), writeResp.GetSha256()) {
		t.Errorf("sha256 mismatch over HTTP between writedisk and readdisk")
	}

	// 7. POST /writeram -> 200 OK, reachable in HTTP mode
	ramReqBytes, _ := proto.Marshal(&gluttonpb.WriteRAMRequest{
		Key:       "httpram",
		Size:      "1Ki",
		WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
	})
	res, err = http.Post(ts.URL+WriteRAMRoute, "application/x-protobuf", bytes.NewReader(ramReqBytes))
	if err != nil {
		t.Fatalf("POST %s failed: %v", WriteRAMRoute, err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST %s status: got %d, want 200", WriteRAMRoute, res.StatusCode)
	}
	res.Body.Close()

	// 8. POST /readram -> 200 OK & matching size
	ramReadReqBytes, _ := proto.Marshal(&gluttonpb.ReadRAMRequest{Key: "httpram"})
	res, err = http.Post(ts.URL+ReadRAMRoute, "application/x-protobuf", bytes.NewReader(ramReadReqBytes))
	if err != nil {
		t.Fatalf("POST %s failed: %v", ReadRAMRoute, err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST %s status: got %d, want 200", ReadRAMRoute, res.StatusCode)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	var ramReadResp gluttonpb.ReadRAMResponse
	if err := proto.Unmarshal(body, &ramReadResp); err != nil {
		t.Fatalf("unmarshal ReadRAMResponse failed: %v", err)
	}
	if ramReadResp.GetSize() != 1024 {
		t.Errorf("ReadRAMResponse size: got %d, want 1024", ramReadResp.GetSize())
	}

	// 9. unknown key -> 404 (NotFound mapping)
	missBytes, _ := proto.Marshal(&gluttonpb.ReadDiskRequest{Key: "nosuchfile"})
	res, err = http.Post(ts.URL+ReadDiskRoute, "application/x-protobuf", bytes.NewReader(missBytes))
	if err != nil {
		t.Fatalf("POST %s miss failed: %v", ReadDiskRoute, err)
	}
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("POST %s miss status: got %d, want 404", ReadDiskRoute, res.StatusCode)
	}
	res.Body.Close()

	// 10. traversal key -> 400 (InvalidArgument mapping)
	badBytes, _ := proto.Marshal(&gluttonpb.ReadDiskRequest{Key: "../etc/passwd"})
	res, err = http.Post(ts.URL+ReadDiskRoute, "application/x-protobuf", bytes.NewReader(badBytes))
	if err != nil {
		t.Fatalf("POST %s bad key failed: %v", ReadDiskRoute, err)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("POST %s bad key status: got %d, want 400", ReadDiskRoute, res.StatusCode)
	}
	res.Body.Close()
}
