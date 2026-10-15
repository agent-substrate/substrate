//go:build linux

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

package tunnel

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

var testActor = resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "space", Name: "actor"}, UID: "uid"}

// fakeClock moves only when advanced.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fetchResult is what one fetch returns.
type fetchResult struct {
	policy *ateletpb.EgressPolicy
	err    error
}

// scriptedFetch returns results in order, one per fetch, and counts them.
type scriptedFetch struct {
	results []fetchResult
	fetches atomic.Int32
}

func (f *scriptedFetch) fetch(context.Context) (*ateletpb.EgressPolicy, error) {
	r := f.results[f.fetches.Add(1)-1]
	return r.policy, r.err
}

// newTestSource returns a source over fetch whose clock moves only when the
// test advances it.
func newTestSource(fetch func(context.Context) (*ateletpb.EgressPolicy, error)) (*actorEgressPolicy, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	return &actorEgressPolicy{
		fetch: fetch,
		ttl:   egressPolicyTTL,
		now:   clock.now,
		log:   slog.New(slog.DiscardHandler),
	}, clock
}

func TestActorEgressPolicyReusesAFetchForItsTTL(t *testing.T) {
	policy := &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{Http: &ateletpb.EgressDestinations{Hostnames: []string{"example.com"}}}}}
	script := &scriptedFetch{results: []fetchResult{{policy: policy}, {policy: policy}}}
	source, clock := newTestSource(script.fetch)

	for _, step := range []struct {
		advance     time.Duration
		wantFetches int32
	}{
		{0, 1},
		{egressPolicyTTL - time.Nanosecond, 1},
		{time.Nanosecond, 2},
	} {
		clock.advance(step.advance)
		got, err := source.EgressPolicy(context.Background())
		if err != nil || got != policy {
			t.Fatalf("EgressPolicy = %v, %v; want %v", got, err, policy)
		}
		if n := script.fetches.Load(); n != step.wantFetches {
			t.Errorf("%d fetches after %v more, want %d", n, step.advance, step.wantFetches)
		}
	}
}

// Having no policy is the answer that refuses an actor's DNS, and it is reused
// like any other.
func TestActorEgressPolicyReusesNoPolicy(t *testing.T) {
	script := &scriptedFetch{results: []fetchResult{{}}}
	source, _ := newTestSource(script.fetch)

	for range 2 {
		got, err := source.EgressPolicy(context.Background())
		if err != nil || got != nil {
			t.Fatalf("EgressPolicy = %v, %v; want nil, nil", got, err)
		}
	}
	if n := script.fetches.Load(); n != 1 {
		t.Errorf("%d fetches, want 1", n)
	}
}

// A failed initial fetch (before any policy is cached) is not reused: the next
// query fetches again rather than being refused for the rest of the TTL.
func TestActorEgressPolicyRetriesAfterAFailure(t *testing.T) {
	errUnavailable := errors.New("atelet is unavailable")
	policy := &ateletpb.EgressPolicy{}
	script := &scriptedFetch{results: []fetchResult{{err: errUnavailable}, {policy: policy}}}
	source, _ := newTestSource(script.fetch)

	if _, err := source.EgressPolicy(context.Background()); !errors.Is(err, errUnavailable) {
		t.Fatalf("EgressPolicy error = %v, want %v", err, errUnavailable)
	}
	got, err := source.EgressPolicy(context.Background())
	if err != nil || got != policy {
		t.Fatalf("EgressPolicy after the failure = %v, %v; want %v", got, err, policy)
	}
}

// Once a policy (or no-policy) has been cached, a failed refresh keeps serving
// the stale cache entry (fail static) until atelet responds again.
func TestActorEgressPolicyFailsStaticOnRefreshFailure(t *testing.T) {
	errUnavailable := errors.New("atelet is unavailable")
	policy1 := &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{Http: &ateletpb.EgressDestinations{Hostnames: []string{"one.example.com"}}}}}
	policy2 := &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{Http: &ateletpb.EgressDestinations{Hostnames: []string{"two.example.com"}}}}}
	script := &scriptedFetch{results: []fetchResult{
		{policy: policy1},
		{err: errUnavailable},
		{err: errUnavailable},
		{policy: policy2},
		{policy: nil},
		{err: errUnavailable},
	}}
	source, clock := newTestSource(script.fetch)

	for i, want := range []*ateletpb.EgressPolicy{policy1, policy1, policy1, policy2, nil, nil} {
		got, err := source.EgressPolicy(context.Background())
		if err != nil || got != want {
			t.Fatalf("step %d: EgressPolicy = %v, %v; want %v, nil", i, got, err, want)
		}
		clock.advance(egressPolicyTTL)
	}
}

// The queries an actor sends together wait on one fetch rather than each
// asking atelet.
func TestActorEgressPolicySharesOneFetch(t *testing.T) {
	policy := &ateletpb.EgressPolicy{}
	started := make(chan struct{})
	release := make(chan struct{})
	var fetches atomic.Int32
	source, _ := newTestSource(func(context.Context) (*ateletpb.EgressPolicy, error) {
		if fetches.Add(1) == 1 {
			close(started)
		}
		<-release
		return policy, nil
	})

	const queries = 8
	errs := make(chan error, queries)
	for range queries {
		go func() {
			got, err := source.EgressPolicy(context.Background())
			if err == nil && got != policy {
				err = fmt.Errorf("EgressPolicy = %v, want %v", got, policy)
			}
			errs <- err
		}()
	}
	<-started
	close(release)
	for range queries {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	if n := fetches.Load(); n != 1 {
		t.Errorf("%d fetches for %d queries at once, want 1", n, queries)
	}
}

// A query stops waiting when its context ends, but the fetch it started goes on
// for the other queries, bounded by its own deadline.
func TestActorEgressPolicyQueryStopsWaitingWhenCanceled(t *testing.T) {
	policy := &ateletpb.EgressPolicy{}
	started := make(chan struct{})
	release := make(chan struct{})
	fetchCtxErr := make(chan error, 1)
	var fetches atomic.Int32
	source, _ := newTestSource(func(ctx context.Context) (*ateletpb.EgressPolicy, error) {
		fetches.Add(1)
		close(started)
		<-release
		if _, ok := ctx.Deadline(); !ok {
			fetchCtxErr <- errors.New("the fetch has no deadline")
		} else {
			fetchCtxErr <- ctx.Err()
		}
		return policy, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := source.EgressPolicy(ctx)
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("EgressPolicy error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the query kept waiting after its context was canceled")
	}

	close(release)
	if err := <-fetchCtxErr; err != nil {
		t.Errorf("the fetch's context: %v; want it to outlive the query", err)
	}
	got, err := source.EgressPolicy(context.Background())
	if err != nil || got != policy {
		t.Fatalf("EgressPolicy = %v, %v; want %v", got, err, policy)
	}
	if n := fetches.Load(); n != 1 {
		t.Errorf("%d fetches, want 1", n)
	}
}

// recordingHandler keeps the level and message of each record.
type recordingHandler struct {
	mu      sync.Mutex
	records []string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Level.String()+" "+r.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// The source logs when the actor gains or loses its policy, an error on the
// first failure in a run, and a warning every 5 minutes while a stale cached
// policy is used.
func TestActorEgressPolicyLogsChanges(t *testing.T) {
	errUnavailable := errors.New("atelet is unavailable")
	policy := &ateletpb.EgressPolicy{}
	steps := []struct {
		advance time.Duration
		result  fetchResult
	}{
		// Initial fetch fails twice before any entry is cached.
		{0, fetchResult{err: errUnavailable}},
		{0, fetchResult{err: errUnavailable}},
		// First successful fetch caches "no policy".
		{0, fetchResult{}},
		{egressPolicyTTL, fetchResult{}},
		// Refresh fails: first failure logs ERROR, 1ns before 5m logs DEBUG,
		// at 5m logs WARN, within the next 5m logs DEBUG, at 10m logs WARN.
		{egressPolicyTTL, fetchResult{err: errUnavailable}},
		{egressPolicyStaleWarnInterval - time.Nanosecond, fetchResult{err: errUnavailable}},
		{time.Nanosecond, fetchResult{err: errUnavailable}},
		{egressPolicyTTL, fetchResult{err: errUnavailable}},
		{egressPolicyStaleWarnInterval - egressPolicyTTL, fetchResult{err: errUnavailable}},
		// Recovery logs INFO and resets failure state.
		{egressPolicyTTL, fetchResult{policy: policy}},
		{egressPolicyTTL, fetchResult{policy: policy}},
		// A new outage logs ERROR again on its first failure.
		{egressPolicyTTL, fetchResult{err: errUnavailable}},
		{egressPolicyTTL, fetchResult{}},
	}
	results := make([]fetchResult, len(steps))
	for i, s := range steps {
		results[i] = s.result
	}
	script := &scriptedFetch{results: results}
	source, clock := newTestSource(script.fetch)
	var logged recordingHandler
	source.log = slog.New(&logged)

	for _, s := range steps {
		clock.advance(s.advance)
		_, _ = source.EgressPolicy(context.Background())
	}
	want := []string{
		"ERROR Could not fetch the actor's egress policy; refusing its DNS queries",
		"DEBUG Could not fetch the actor's egress policy; refusing its DNS queries",
		"INFO Actor has no egress policy; blocking all DNS queries",
		"ERROR Could not refresh the actor's egress policy; using stale cached policy",
		"DEBUG Could not refresh the actor's egress policy; using stale cached policy",
		"WARN Could not refresh the actor's egress policy; using stale cached policy",
		"DEBUG Could not refresh the actor's egress policy; using stale cached policy",
		"WARN Could not refresh the actor's egress policy; using stale cached policy",
		"INFO Actor has an egress policy",
		"ERROR Could not refresh the actor's egress policy; using stale cached policy",
		"INFO Actor has no egress policy; blocking all DNS queries",
	}
	logged.mu.Lock()
	defer logged.mu.Unlock()
	if diff := cmp.Diff(want, logged.records); diff != "" {
		t.Errorf("records mismatch (-want +got):\n%s", diff)
	}
}

// fakeSupportClient answers egress policy requests with resp or err.
type fakeSupportClient struct {
	ateletpb.WorkerSupportClient
	resp *ateletpb.GetActorEgressPolicyResponse
	err  error
	got  *ateletpb.GetActorEgressPolicyRequest
}

func (c *fakeSupportClient) GetActorEgressPolicy(_ context.Context, req *ateletpb.GetActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateletpb.GetActorEgressPolicyResponse, error) {
	c.got = req
	return c.resp, c.err
}

func TestGetEgressPolicy(t *testing.T) {
	policy := &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
		Https: &ateletpb.EgressDestinations{Hostnames: []string{"example.com"}},
	}}}
	errUnavailable := status.Error(codes.Unavailable, "the control plane is unavailable")

	for _, tc := range []struct {
		name    string
		resp    *ateletpb.GetActorEgressPolicyResponse
		err     error
		want    *ateletpb.EgressPolicy
		wantErr bool
	}{
		{name: "policy", resp: &ateletpb.GetActorEgressPolicyResponse{EgressPolicy: policy}, want: policy},
		{name: "no policy", resp: &ateletpb.GetActorEgressPolicyResponse{}},
		{name: "atelet error", err: errUnavailable, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeSupportClient{resp: tc.resp, err: tc.err}
			got, err := getEgressPolicy(context.Background(), client, testActor.Ref)
			if (err != nil) != tc.wantErr {
				t.Fatalf("getEgressPolicy error = %v, wantErr %t", err, tc.wantErr)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Errorf("getEgressPolicy error = %v, want it to wrap %v", err, tc.err)
			}
			if (got == nil) != (tc.want == nil) {
				t.Fatalf("getEgressPolicy = %v, want %v", got, tc.want)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("policy mismatch (-want +got):\n%s", diff)
			}
			wantReq := &ateletpb.GetActorEgressPolicyRequest{ActorAtespace: "space", ActorName: "actor"}
			if diff := cmp.Diff(wantReq, client.got, protocmp.Transform()); diff != "" {
				t.Errorf("request mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// fakeAtelet serves an egress policy response as the node's atelet would.
type fakeAtelet struct {
	ateletpb.UnimplementedWorkerSupportServer

	mu   sync.Mutex
	resp *ateletpb.GetActorEgressPolicyResponse
	got  []*ateletpb.GetActorEgressPolicyRequest
}

func (a *fakeAtelet) GetActorEgressPolicy(_ context.Context, req *ateletpb.GetActorEgressPolicyRequest) (*ateletpb.GetActorEgressPolicyResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.got = append(a.got, req)
	return a.resp, nil
}

func (a *fakeAtelet) respond(resp *ateletpb.GetActorEgressPolicyResponse) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resp = resp
}

// serveAtelet serves a fake atelet on the WorkerSupport socket, presenting a
// certificate issued by ca to the atelet identity ateletID on the test node.
func serveAtelet(t *testing.T, ca testCA, ateletID string) *fakeAtelet {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := url.Parse(ateletID)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		URIs:         []*url.URL{id},
	}
	if err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: "atelet", ServiceAccountUID: "atelet-sa-uid",
		PodName: "atelet", PodUID: "atelet-uid", NodeName: testNodeName, NodeUID: testNodeUID,
	}, template); err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(ca.cert)

	// t.TempDir() embeds the test name, which can overrun the unix socket
	// path limit, so the socket gets its own short directory.
	socketDir, err := os.MkdirTemp("", "tunnel")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "support.sock")
	old := nodepath.WorkerSupportSocket
	nodepath.WorkerSupportSocket = socket
	t.Cleanup(func() { nodepath.WorkerSupportSocket = old })

	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})))
	atelet := &fakeAtelet{}
	ateletpb.RegisterWorkerSupportServer(server, atelet)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return atelet
}

// The tunnel fetches an actor's policy from the atelet on its node, over the
// worker's own credentials.
func TestTunnelFetchesActorEgressPolicyFromAtelet(t *testing.T) {
	cfg, ca := testConfigAndCA(t)
	tunnel := startTunnel(t, cfg)
	atelet := serveAtelet(t, ca, cfg.BrokerIdentity)

	policy := &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
		Https: &ateletpb.EgressDestinations{Hostnames: []string{"example.com"}},
	}}}
	for _, tc := range []struct {
		name string
		resp *ateletpb.GetActorEgressPolicyResponse
		want *ateletpb.EgressPolicy
	}{
		{name: "policy", resp: &ateletpb.GetActorEgressPolicyResponse{EgressPolicy: policy}, want: policy},
		// An empty policy encodes to no bytes, and must still arrive as a policy.
		{name: "empty policy", resp: &ateletpb.GetActorEgressPolicyResponse{EgressPolicy: &ateletpb.EgressPolicy{}}, want: &ateletpb.EgressPolicy{}},
		{name: "no policy", resp: &ateletpb.GetActorEgressPolicyResponse{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			atelet.respond(tc.resp)
			got, err := tunnel.ActorEgressPolicy(testActor).EgressPolicy(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if (got == nil) != (tc.want == nil) {
				t.Fatalf("EgressPolicy = %v, want %v", got, tc.want)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("policy mismatch (-want +got):\n%s", diff)
			}
		})
	}

	atelet.mu.Lock()
	defer atelet.mu.Unlock()
	if len(atelet.got) != 3 {
		t.Fatalf("atelet was asked %d times, want 3", len(atelet.got))
	}
	want := &ateletpb.GetActorEgressPolicyRequest{ActorAtespace: "space", ActorName: "actor"}
	for _, got := range atelet.got {
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("request mismatch (-want +got):\n%s", diff)
		}
	}
}
