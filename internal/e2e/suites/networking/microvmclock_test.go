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
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestMicroVMClockAfterDelayedRestore probes delayed-restore TLS behavior by
// issuing the origin trust chain only after a suspended actor has been idle.
// The only wait here is the deliberate interval; ingress readiness is checked
// separately before the one measured HTTPS operation.
func TestMicroVMClockAfterDelayedRestore(t *testing.T) {
	if !e2e.IsMicroVM() {
		t.Skip("micro-VM clock contract")
	}
	if os.Getenv("E2E_EGRESS_MITM") != "" {
		t.Skip("default passthrough trust contract")
	}

	ctx := t.Context()
	actorName, router, actorRef := prepareProtocolActor(t, ctx, "clock")
	clients := e2e.GetClients()
	t.Logf("clock evidence host_utc_before_suspend=%s actor=%s/%s", time.Now().UTC().Format(time.RFC3339Nano), actorRef.Atespace, actorName)
	beforeSuspend, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef.ToObjectRef()})
	if err != nil {
		t.Fatalf("GetActor before SuspendActor: %v", err)
	}
	if assignment := beforeSuspend.GetStatus().GetWorkerAssignment(); assignment == nil || assignment.GetWorker().GetName() == "" {
		t.Fatalf("actor had no worker assignment before SuspendActor: %v", assignment)
	}
	suspendBoundary := time.Now().UTC()
	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	if actor := suspended.GetActor(); actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("SuspendActor returned state %s, want SUSPENDED", actor.GetStatus().GetState())
	} else if actor.GetStatus().GetWorkerAssignment() != nil {
		t.Fatalf("SuspendActor returned worker assignment %v, want it cleared", actor.GetStatus().GetWorkerAssignment())
	}

	// Keep this interval long enough to separate the snapshot boundary from the
	// fresh certificate issuance and resumed request.
	const delay = 70 * time.Second
	t.Logf("clock evidence suspended; waiting %s before issuing a fresh root and leaf", delay)
	timer := time.NewTimer(delay)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		t.Fatal(ctx.Err())
	}
	t.Logf("clock evidence host_utc_after_delay=%s", time.Now().UTC().Format(time.RFC3339Nano))

	target, address, rootCA := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name: "clock-origin", ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args: []string{"http", "--body=delayed restore clock"}, Port: 8443,
	}, true)
	logOriginCertificateValidity(t, ctx, target, rootCA, suspendBoundary)

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	waitForRouteReady(t, "restored actor ingress readiness", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	since := metav1.NewTime(time.Now().Add(-time.Minute))
	raw := postEgressOnce(t, ctx, router, actorRef, "/", map[string]any{
		"url": "https://" + address + "/healthz", "rootCA": rootCA, "http1": true,
	})
	var got struct {
		StatusCode int    `json:"statusCode"`
		Body       string `json:"body"`
		Protocol   string `json:"protocol"`
		TLS        bool   `json:"tls"`
		Error      string `json:"error"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding delayed-restore HTTPS observation: %v", err)
	}
	if got.Error != "" || got.StatusCode != 200 || got.Body != "delayed restore clock" || got.Protocol != "HTTP/1.1" || !got.TLS {
		t.Errorf("delayed-restore HTTPS observation = %+v; want one verified TLS HTTP/1.1 8443 request", got)
	}
	assertProtocolGateway(t, ctx, since, actorName, target.Address())
}

func logOriginCertificateValidity(t *testing.T, ctx context.Context, target e2e.Server, rootCA string, suspendBoundary time.Time) {
	t.Helper()
	rootBlock, _ := pem.Decode([]byte(rootCA))
	if rootBlock == nil {
		t.Fatalf("origin root CA is not PEM")
	}
	root, err := x509.ParseCertificate(rootBlock.Bytes)
	if err != nil {
		t.Fatalf("parsing origin root CA: %v", err)
	}
	if !root.NotBefore.After(suspendBoundary) {
		t.Fatalf("origin root NotBefore %s is not after suspend boundary %s", root.NotBefore.UTC(), suspendBoundary)
	}
	secret, err := e2e.GetClients().K8s.CoreV1().Secrets(target.Namespace).Get(ctx, "origin-tls", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading origin leaf metadata: %v", err)
	}
	leaf, err := x509.ParseCertificate(mustPEMCertificate(t, secret.Data[corev1.TLSCertKey]))
	if err != nil {
		t.Fatalf("parsing origin leaf: %v", err)
	}
	t.Logf("clock evidence host_utc_after_origin=%s root_not_before=%s root_not_after=%s leaf_not_before=%s leaf_not_after=%s", time.Now().UTC().Format(time.RFC3339Nano), root.NotBefore.UTC().Format(time.RFC3339Nano), root.NotAfter.UTC().Format(time.RFC3339Nano), leaf.NotBefore.UTC().Format(time.RFC3339Nano), leaf.NotAfter.UTC().Format(time.RFC3339Nano))
}

func mustPEMCertificate(t *testing.T, data []byte) []byte {
	t.Helper()
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("origin leaf is not PEM")
	}
	return block.Bytes
}
