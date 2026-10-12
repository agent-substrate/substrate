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

package egress

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// refusalSeries is one atenet.router.egress.refused series.
type refusalSeries struct {
	tmplNs, tmplName, reason string
}

func newTestMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(mp.Meter(extproc.ServiceName))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	return m, reader
}

// collectRefusals returns the count of each refusal series, checking the
// instrument's shape and that every series carries exactly its three labels.
func collectRefusals(t *testing.T, reader *sdkmetric.ManualReader) map[refusalSeries]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := map[refusalSeries]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != refusedMetricName {
				continue
			}
			if m.Unit != "{request}" {
				t.Errorf("%s unit = %q, want {request}", refusedMetricName, m.Unit)
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || !sum.IsMonotonic {
				t.Fatalf("%s is %T, want a monotonic int64 sum", refusedMetricName, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if n := dp.Attributes.Len(); n != 3 {
					t.Errorf("%s series has %d labels, want 3: %v", refusedMetricName, n, dp.Attributes.ToSlice())
				}
				tmplNs, _ := dp.Attributes.Value(ateattr.TemplateAtespaceKey)
				tmplName, _ := dp.Attributes.Value(ateattr.TemplateNameKey)
				reason, _ := dp.Attributes.Value(ateattr.EgressRefusalReasonKey)
				got[refusalSeries{tmplNs.AsString(), tmplName.AsString(), reason.AsString()}] += dp.Value
			}
		}
	}
	return got
}

// templatedActor is the test actor in state, created from the coder template.
func templatedActor(state ateapipb.ActorState) *ateapipb.Actor {
	actor := runningActor()
	actor.ActorTemplate = &ateapipb.ObjectRef{Atespace: "ate-agents", Name: "coder"}
	actor.Status.State = state
	return actor
}

func withWebSocketUpgrade(md *extproc.RequestMetadata) *extproc.RequestMetadata {
	md.Headers["connection"] = "Upgrade"
	md.Headers["upgrade"] = "websocket"
	return md
}

// Every refusal counts once, labeled by the template of the actor and the
// reason. A refusal inside a tunnel names the template the CONNECT learned.
func TestRefusalsCountedByTemplateAndReason(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, testActorSPIFFEID, actorCertOptions{})
	running := templatedActor(ateapipb.ActorState_ACTOR_STATE_RUNNING)
	coder := func(reason string) refusalSeries { return refusalSeries{"ate-agents", "coder", reason} }
	unknown := func(reason string) refusalSeries {
		return refusalSeries{ateattr.TemplateUnknown, ateattr.TemplateUnknown, reason}
	}
	mitm := extproc.EgressTLSMITMFilterChainName

	tests := []struct {
		name     string
		client   *egressMockClient
		provider credproviderpb.CredentialProviderClient
		// connect sends the CONNECT that opens the tunnel first.
		connect bool
		// request is the callout refused; nil means the CONNECT is.
		request *extproc.RequestMetadata
		want    refusalSeries
	}{
		{
			name:    "destination outside the policy",
			client:  &egressMockClient{actor: running, policy: httpPolicy("api.example.com")},
			connect: true,
			request: requestMetadata("exfil.example.org"),
			want:    coder(ateattr.EgressRefusalNotInPolicy),
		},
		{
			name:    "SNI outside the policy",
			client:  &egressMockClient{actor: running, policy: httpsPolicy("api.example.com")},
			connect: true,
			request: innerMetadata(mitm, "GET", "api.example.com", map[string]string{extproc.RequestedServerNameAttribute: "exfil.example.org"}),
			want:    coder(ateattr.EgressRefusalNotInPolicy),
		},
		{
			name:    "no policy",
			client:  &egressMockClient{actor: running},
			connect: true,
			want:    coder(ateattr.EgressRefusalNoPolicy),
		},
		{
			name:    "policy without rules",
			client:  &egressMockClient{actor: running, policy: &ateapipb.EgressPolicy{}},
			connect: true,
			want:    coder(ateattr.EgressRefusalNoPolicy),
		},
		{
			name:    "policy lookup failed",
			client:  &egressMockClient{actor: running, policyErr: status.Error(codes.Unavailable, "ateapi down")},
			connect: true,
			want:    coder(ateattr.EgressRefusalUnavailable),
		},
		{
			name:    "actor not running",
			client:  &egressMockClient{actor: templatedActor(ateapipb.ActorState_ACTOR_STATE_SUSPENDED), policy: allowAllPolicy()},
			connect: true,
			want:    coder(ateattr.EgressRefusalActorNotRunning),
		},
		{
			name:    "actor deleted",
			client:  &egressMockClient{err: status.Error(codes.NotFound, "no such actor")},
			connect: true,
			want:    unknown(ateattr.EgressRefusalUnknownActor),
		},
		{
			name:    "actor lookup failed",
			client:  &egressMockClient{err: status.Error(codes.Unavailable, "ateapi down")},
			connect: true,
			want:    unknown(ateattr.EgressRefusalUnavailable),
		},
		{
			name:    "CONNECT without a certificate",
			client:  &egressMockClient{actor: running, policy: allowAllPolicy()},
			request: egressMetadata(""),
			want:    unknown(ateattr.EgressRefusalUnauthenticated),
		},
		{
			name:    "request without an actor identity",
			client:  &egressMockClient{actor: running, policy: allowAllPolicy()},
			connect: true,
			request: innerMetadata(extproc.EgressCleartextFilterChainName, "GET", "api.example.com", map[string]string{extproc.ActorIdentityFilterStateAttribute: ""}),
			want:    unknown(ateattr.EgressRefusalUnauthenticated),
		},
		{
			name:    "request naming no destination",
			client:  &egressMockClient{actor: running, policy: allowAllPolicy()},
			connect: true,
			request: innerMetadata(extproc.EgressCleartextFilterChainName, "GET", "api.example.com", dialed("api.example.com:80")),
			want:    coder(ateattr.EgressRefusalBadDestination),
		},
		{
			name:    "WebSocket upgrade",
			client:  &egressMockClient{actor: running, policy: allowAllPolicy()},
			connect: true,
			request: withWebSocketUpgrade(requestMetadata("api.example.com")),
			want:    coder(ateattr.EgressRefusalWebSocket),
		},
		{
			name:     "credential fetch failed",
			client:   &egressMockClient{actor: running, policy: credentialInjectionPolicySample("api.example.com")},
			provider: &fakeProvider{err: errors.New("provider down")},
			connect:  true,
			request:  withAuthorization(innerMetadata(mitm, "GET", "api.example.com", nil)),
			want:     coder(ateattr.EgressRefusalCredential),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, reader := newTestMetrics(t)
			h := New(tc.client, ca.roots(), 0, tc.provider, injectionProviderName).WithMetrics(m)
			ctx := context.Background()

			var err error
			if tc.connect {
				_, err = h.HandleRequestHeaders(ctx, egressMetadata(xfccHeader(leaf)))
			}
			if tc.request != nil {
				if err != nil {
					t.Fatalf("CONNECT refused: %v", err)
				}
				_, err = h.HandleRequestHeaders(ctx, tc.request)
			}
			if err == nil {
				t.Fatal("the request was allowed, want a refusal")
			}

			got := collectRefusals(t, reader)
			if len(got) != 1 || got[tc.want] != 1 {
				t.Errorf("refusals = %v, want one %v", got, tc.want)
			}
		})
	}
}

func TestAllowedRequestsAreNotCounted(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, testActorSPIFFEID, actorCertOptions{})
	m, reader := newTestMetrics(t)
	h := New(&egressMockClient{actor: templatedActor(ateapipb.ActorState_ACTOR_STATE_RUNNING), policy: allowAllPolicy()}, ca.roots(), 0, nil, "").WithMetrics(m)

	for _, md := range []*extproc.RequestMetadata{egressMetadata(xfccHeader(leaf)), requestMetadata("api.example.com")} {
		if _, err := h.HandleRequestHeaders(context.Background(), md); err != nil {
			t.Fatalf("HandleRequestHeaders: %v", err)
		}
	}
	if got := collectRefusals(t, reader); len(got) != 0 {
		t.Errorf("refusals = %v, want none", got)
	}
}

// A caller that gave up has not been refused.
func TestAbandonedCalloutIsNotCounted(t *testing.T) {
	m, reader := newTestMetrics(t)
	client := &egressMockClient{actor: runningActor(), policy: allowAllPolicy(), policyGate: make(chan struct{})}
	defer close(client.policyGate)
	h := New(client, nil, 0, nil, "").WithMetrics(m)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.HandleRequestHeaders(ctx, requestMetadata("api.example.com")); err == nil {
		t.Fatal("an abandoned callout was allowed")
	}
	if got := collectRefusals(t, reader); len(got) != 0 {
		t.Errorf("refusals = %v, want none", got)
	}
}

func TestHandlerWithoutMetricsRefuses(t *testing.T) {
	h := policyHandler(httpPolicy("api.example.com"))
	_, err := h.HandleRequestHeaders(context.Background(), requestMetadata("exfil.example.org"))
	if err == nil {
		t.Fatal("the request was allowed, want a refusal")
	}
}

func TestActorTemplatesForgetIdleActors(t *testing.T) {
	templates := newActorTemplates()
	now := time.Unix(0, 0)
	templates.now = func() time.Time { return now }
	template := &ateapipb.ObjectRef{Atespace: "ate-agents", Name: "coder"}

	idle := resources.ActorRef{Atespace: "default", Name: "idle"}
	busy := resources.ActorRef{Atespace: "default", Name: "busy"}
	templates.remember(idle, template)
	templates.remember(busy, template)
	if ns, name := templates.lookup(resources.ActorRef{Atespace: "default", Name: "never-seen"}); ns != "" || name != "" {
		t.Errorf("lookup of an actor never seen = %q/%q, want empty", ns, name)
	}

	now = now.Add(templateMemory / 2)
	templates.lookup(busy)
	now = now.Add(templateMemory/2 + time.Second)
	// Fill the map to its cleanup size, so the next store walks it.
	for i := len(templates.entries); i < minTemplateCleanupSize; i++ {
		templates.remember(resources.ActorRef{Atespace: "default", Name: "actor-" + strconv.Itoa(i)}, template)
	}

	if _, ok := templates.entries[idle]; ok {
		t.Error("an actor idle past templateMemory is still remembered")
	}
	if ns, name := templates.lookup(busy); ns != "ate-agents" || name != "coder" {
		t.Errorf("lookup(busy) = %q/%q, want ate-agents/coder", ns, name)
	}
}
