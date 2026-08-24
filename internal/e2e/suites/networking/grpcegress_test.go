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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// grpcEcho is the origin this test deploys: a cleartext-HTTP/2 gRPC server, in
// its own namespace, so nothing here depends on the internet.
var grpcEcho = e2e.ServerPod{
	Name:       "grpcecho",
	ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/grpcecho",
	Port:       50051,
	// A gRPC server answers an HTTP GET with a protocol error, so readiness has
	// to go through the health service the fixture registers.
	GRPCProbe: true,
}

// grpcEchoResponse mirrors the egress demo Actor's /grpc answer. Kept as a
// local copy rather than imported: the demo is a separate module-internal
// command, and what this suite is really pinning is the wire shape between the
// two, which a shared struct would hide.
type grpcEchoResponse struct {
	Message string                `json:"message"`
	Stream  []grpcEchoStreamedMsg `json:"stream"`
	Bidi    []grpcEchoStreamedMsg `json:"bidi"`
	Code    string                `json:"code"`
	Error   string                `json:"error"`
}

type grpcEchoStreamedMsg struct {
	Message string `json:"message"`
	Index   int32  `json:"index"`
}

// TestActorEgressGRPC covers the egress path with gRPC, which fails in ways the
// HTTP tests cannot see. atenet-egress terminates the Actor's CONNECT and
// relays opaque TCP, so HTTP/2 framing has to survive end to end and the gRPC
// status has to arrive in trailers, after the response body. An egress path
// that parsed the traffic as HTTP/1.1, or dropped trailers, would still pass
// TestActorEgress and fail here.
//
// All three streaming shapes in one request, because each one fails
// differently: unary is a status in trailers, a server-stream is many frames
// over a held-open connection, and a bidirectional stream has both directions
// carrying frames at once and then half-closes one of them.
//
// The origin is an in-cluster cleartext-HTTP/2 server, deployed per test into
// its own namespace, so nothing in this test depends on the internet.
func TestActorEgressGRPC(t *testing.T) {
	ctx := context.Background()
	target := e2e.DeployServerPod(t, ctx, grpcEcho).Address()

	actorName, _ := createAndResumeActor(t, ctx, "egress-grpc", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	// Bound the access-log scan below to lines this test could have produced.
	// The slack absorbs clock skew between here and the gateway's node.
	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	const (
		message     = "hello over grpc"
		streamCount = 3
		bidiCount   = 3
	)
	payload, err := json.Marshal(map[string]any{
		"target":      target,
		"message":     message,
		"streamCount": streamCount,
		"bidiCount":   bidiCount,
	})
	if err != nil {
		t.Fatalf("marshaling the gRPC request for %s: %v", target, err)
	}

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	status, body := postThroughEgressActor(t, ctx, router, actorRef, "/grpc", payload)
	if status != http.StatusOK {
		t.Fatalf("Actor gRPC egress to %s returned HTTP %d, want 200; body: %s", target, status, body)
	}

	var got grpcEchoResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decoding the Actor's gRPC response: %v; body: %s", err, body)
	}
	if got.Message != message {
		t.Errorf("unary Echo returned %q, want %q", got.Message, message)
	}
	// The status is the trailer assertion: a gRPC status is sent after the
	// response body, so a path that dropped trailers could deliver the message
	// above and still not produce this.
	if got.Code != "OK" {
		t.Errorf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
	}
	if len(got.Stream) != streamCount {
		t.Fatalf("EchoStream returned %d responses, want %d: %+v", len(got.Stream), streamCount, got.Stream)
	}
	for i, response := range got.Stream {
		if response.Message != message {
			t.Errorf("stream response %d message = %q, want %q", i, response.Message, message)
		}
		if int(response.Index) != i {
			t.Errorf("stream response %d index = %d, want %d", i, response.Index, i)
		}
	}

	// The bidi leg is the one that needs frames moving in both directions at
	// once: the Actor sends each message only after reading the response to the
	// previous one, so a path that carried one direction at a time would not
	// return a short answer here, it would hang until the Actor's own 15s
	// timeout and come back as a 502.
	if len(got.Bidi) != bidiCount {
		t.Fatalf("EchoBidi returned %d responses, want %d: %+v", len(got.Bidi), bidiCount, got.Bidi)
	}
	for i, response := range got.Bidi {
		// The Actor numbers each message it sends, so this pins every response
		// to the request it answered.
		want := fmt.Sprintf("%s-%d", message, i)
		if response.Message != want {
			t.Errorf("bidi response %d message = %q, want %q", i, response.Message, want)
		}
		if int(response.Index) != i {
			t.Errorf("bidi response %d index = %d, want %d", i, response.Index, i)
		}
	}
	t.Logf("Actor gRPC egress to %s succeeded; body: %s", target, body)

	// Everything above would also pass if the Actor's traffic had been
	// masqueraded straight out instead of tunneled. This is what says it went
	// through the gateway, on this Actor's own certificate.
	assertEgressGatewayConnect(t, ctx, since, actorName, strconv.Itoa(grpcEcho.Port))
}
