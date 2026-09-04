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

package actoridentity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
}

const (
	testAtespace  = "team-alpha"
	testActorName = "counter-1"
	testPodNS     = "ate-workers"
	testWorkerPod = "worker-abc"
	// testWorkerName is the seeded worker's resource name, and so the name
	// MintCert requests reference it by. It is deliberately not equal to
	// testWorkerPodUID: MintCert must resolve the worker by name alone.
	testWorkerName   = "5b1e0c7a-8d34-4f62-b0a9-1e7c4d29f350"
	testWorkerPodUID = "e2c40f8b-71d9-4a35-8c6e-b04f9d1a7263"
	testPool         = "pool-1"
	testNode         = "node-a"
	testOtherNode    = "node-b"
)

// newTestCert builds a self-signed leaf carrying the given SPIFFE URI path
// (skipped when empty) and, when podIdentity is non-nil, a PodIdentity
// extension.
//
// The certificate is created and then re-parsed on purpose:
// AddPodIdentityToCertificate writes to ExtraExtensions, but
// PodIdentityFromCertificate reads Extensions, which only x509.ParseCertificate
// populates. Self-signing is sufficient because the code under test reads an
// already transport-verified peer certificate and never re-validates the chain
// itself.

// newTestServer returns a Server backed by st, with a freshly generated actor
// CA pool written to a temp file.
func newTestServer(t *testing.T, st store.Interface) *Server {
	t.Helper()

	certificateAuthority, err := localca.GenerateCA("test-actor-ca", localca.KeyTypeED25519, 24*time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	certificateAuthorityPool := &localca.ConcretePool{
		CAs:              []*localca.CA{certificateAuthority},
		ActiveForSigning: "test-actor-ca",
	}

	jwtAuthority, err := localjwtauthority.GenerateECDSAP256Authority("1")
	if err != nil {
		t.Fatalf("while generating JWT authority: %v", err)
	}
	jwtAuthorityPool := &localjwtauthority.ConcretePool{
		Authorities:      []*localjwtauthority.Authority{jwtAuthority},
		ActiveForSigning: "1",
	}

	var workers *workercache.Cache
	if st != nil {
		workers = workercache.New(st, time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := workers.Start(ctx); err != nil {
			t.Fatalf("start worker cache: %v", err)
		}
	}
	return New("issuer", jwtAuthorityPool, certificateAuthorityPool, st, workers)
}

// staleWatchStore wraps a store with a WatchWorkers that never delivers,
// freezing any workercache built over it at its seed-time state — the unit
// analog of the watch's delivery latency.
type staleWatchStore struct{ store.Interface }

func (s staleWatchStore) WatchWorkers(context.Context) (*store.WorkerWatch, error) {
	return store.NewWorkerWatch(make(chan store.WorkerEvent), func() {}), nil
}

// TestMintCertReadsThroughStaleWorkerCache pins the authorization
// read-through: an atelet minting immediately after ResumeActor committed the
// worker's assignment must be authorized from the store even though this
// replica's cache has not yet seen the assignment. The control case keeps the
// store unassigned too and must still deny — only fresh data may authorize,
// and only fresh data may deny.
func TestMintCertReadsThroughStaleWorkerCache(t *testing.T) {
	for name, assignInStore := range map[string]bool{
		"assignment committed but not yet in cache: authorized via read-through": true,
		"unassigned in cache and store: denial stands":                           false,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			// Phase 1: worker exists, unassigned; the cache seeds this view and
			// (via the inert watch) never learns anything newer.
			seedActor(t, ctx, st, actorFixture{state: ateapipb.ActorState_ACTOR_STATE_RUNNING, workerNode: testNode, unassigned: true})
			workers := workercache.New(staleWatchStore{st}, time.Hour)
			cacheCtx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			if err := workers.Start(cacheCtx); err != nil {
				t.Fatalf("start worker cache: %v", err)
			}

			actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
			if err != nil {
				t.Fatalf("read seeded actor: %v", err)
			}
			if assignInStore {
				// Phase 2: commit the assignment to the store only, as
				// AssignWorker does (possibly on another replica).
				bindActor(t, ctx, st, testWorkerName, &ateapipb.ActorAssignment{
					Actor:    (resources.ActorRef{Atespace: testAtespace, Name: testActorName}).ToObjectRef(),
					ActorUid: actor.GetMetadata().GetUid(),
				})
			}

			srv := newTestServerWithCache(t, st, workers)
			resp, err := srv.MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), mintCertRequest(t, actor.GetMetadata().GetUid()))

			wantCode := codes.PermissionDenied
			if assignInStore {
				wantCode = codes.OK
			}
			if got := status.Code(err); got != wantCode {
				t.Fatalf("MintCert() code = %v (err = %v), want %v", got, err, wantCode)
			}
			if assignInStore && len(resp.GetActorCertificates()) == 0 {
				t.Fatal("MintCert() returned no certificates")
			}
		})
	}
}

// The multi-actor case of the same lag: the cached worker is not unassigned but
// hosting somebody else, so the requested actor's absence looks like an answer
// rather than a miss.
func TestMintCertReadsThroughForAnActorTheCacheHasNotSeenYet(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	// The cache seeds with the worker hosting only the first actor, and (via
	// the inert watch) never learns of the second.
	seedActor(t, ctx, st, actorFixture{state: ateapipb.ActorState_ACTOR_STATE_RUNNING, workerNode: testNode})
	workers := workercache.New(staleWatchStore{st}, time.Hour)
	cacheCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	if err := workers.Start(cacheCtx); err != nil {
		t.Fatalf("start worker cache: %v", err)
	}

	const secondActorName = "counter-2"
	second, err := st.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: secondActorName},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: testWorkerName},
				WorkerNamespace: testPodNS,
				WorkerPool:      testPool,
				WorkerPod:       testWorkerPod,
				WorkerPodUid:    testWorkerPodUID,
			},
		},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ate-demo", Name: "counter"},
	})
	if err != nil {
		t.Fatalf("seed second actor: %v", err)
	}

	// Bind it to the worker in the store only, as AssignWorker does.
	bindActor(t, ctx, st, testWorkerName, &ateapipb.ActorAssignment{
		Actor:    (resources.ActorRef{Atespace: testAtespace, Name: secondActorName}).ToObjectRef(),
		ActorUid: second.GetMetadata().GetUid(),
	})

	srv := newTestServerWithCache(t, st, workers)
	resp, err := srv.MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), mintCertRequest(t, second.GetMetadata().GetUid()))
	if err != nil {
		t.Fatalf("MintCert() for an actor the cache has not seen: %v", err)
	}
	if len(resp.GetActorCertificates()) == 0 {
		t.Fatal("MintCert() returned no certificates")
	}

	leaf, err := x509.ParseCertificate(resp.GetActorCertificates()[0])
	if err != nil {
		t.Fatalf("parse minted certificate: %v", err)
	}
	identity, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil {
		t.Fatalf("ActorIdentityFromCertificate: %v", err)
	}
	if identity == nil || identity.ActorName != secondActorName {
		t.Errorf("minted for %+v, want actor %q", identity, secondActorName)
	}
}

// TestMintCertReadsThroughWorkerCacheMiss pins the read-through for a worker
// the cache has never seen: a worker registered moments before assignment may
// be committed to the store (possibly by another replica) before this
// replica's cache has received the worker row at all. Absence from the cache
// is stale data and must not deny by itself; absence from the store must.
func TestMintCertReadsThroughWorkerCacheMiss(t *testing.T) {
	for name, workerInStore := range map[string]bool{
		"worker assigned in store but not yet in cache: authorized via read-through": true,
		"worker in neither cache nor store: denial stands":                           false,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			// Phase 1: only the actor exists; the cache seeds with no workers
			// and (via the inert watch) never learns of any.
			seedActor(t, ctx, st, actorFixture{state: ateapipb.ActorState_ACTOR_STATE_RUNNING, workerNode: testNode, noWorker: true})
			workers := workercache.New(staleWatchStore{st}, time.Hour)
			cacheCtx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			if err := workers.Start(cacheCtx); err != nil {
				t.Fatalf("start worker cache: %v", err)
			}

			actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
			if err != nil {
				t.Fatalf("read seeded actor: %v", err)
			}
			if workerInStore {
				// Phase 2: register and assign the worker in the store only,
				// after the cache stopped listening.
				if _, err := st.CreateWorker(ctx, &ateapipb.Worker{
					Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerName},
					WorkerNamespace: testPodNS,
					WorkerPool:      testPool,
					WorkerPod:       testWorkerPod,
					WorkerPodUid:    testWorkerPodUID,
					NodeName:        testNode,
					Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
				}); err != nil {
					t.Fatalf("register worker in store: %v", err)
				}
				bindActor(t, ctx, st, testWorkerName, &ateapipb.ActorAssignment{
					Actor:    (resources.ActorRef{Atespace: testAtespace, Name: testActorName}).ToObjectRef(),
					ActorUid: actor.GetMetadata().GetUid(),
				})
			}

			srv := newTestServerWithCache(t, st, workers)
			resp, err := srv.MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), mintCertRequest(t, actor.GetMetadata().GetUid()))

			wantCode := codes.PermissionDenied
			if workerInStore {
				wantCode = codes.OK
			}
			if got := status.Code(err); got != wantCode {
				t.Fatalf("MintCert() code = %v (err = %v), want %v", got, err, wantCode)
			}
			if workerInStore && len(resp.GetActorCertificates()) == 0 {
				t.Fatal("MintCert() returned no certificates")
			}
		})
	}
}

// newTestServerWithCache is newTestServer with a caller-controlled worker
// cache (e.g. one frozen at a stale state).
func newTestServerWithCache(t *testing.T, st store.Interface, workers *workercache.Cache) *Server {
	t.Helper()

	certificateAuthority, err := localca.GenerateCA("test-actor-ca", localca.KeyTypeED25519, 24*time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	certificateAuthorityPool := &localca.ConcretePool{
		CAs:              []*localca.CA{certificateAuthority},
		ActiveForSigning: "test-actor-ca",
	}

	jwtAuthority, err := localjwtauthority.GenerateECDSAP256Authority("1")
	if err != nil {
		t.Fatalf("while generating JWT authority: %v", err)
	}
	jwtAuthorityPool := &localjwtauthority.ConcretePool{
		Authorities:      []*localjwtauthority.Authority{jwtAuthority},
		ActiveForSigning: "1",
	}

	return New("issuer", jwtAuthorityPool, certificateAuthorityPool, st, workers)
}

func TestMintJWTRequiresConfiguredJWTProvider(t *testing.T) {
	srv := &Server{actorIdentityJWTIssuer: "https://kubernetes.example"}
	for _, tt := range []struct {
		name string
		ctx  context.Context
		code codes.Code
	}{
		{name: "no principal", ctx: context.Background(), code: codes.Unauthenticated},
		{
			name: "mTLS principal",
			ctx:  principal.InjectContext(context.Background(), principal.PrincipalInfo{ID: "spiffe://caller", Kind: principal.KindMTLS}),
			code: codes.Unauthenticated,
		},
		{
			name: "different JWT provider",
			ctx:  principal.InjectContext(context.Background(), principal.PrincipalInfo{ID: "user", Kind: principal.KindJWT, Issuer: "https://accounts.google.com"}),
			code: codes.PermissionDenied,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := srv.MintJWT(tt.ctx, &ateapipb.MintJWTRequest{})
			if got := status.Code(err); got != tt.code {
				t.Fatalf("MintJWT() code = %v, want %v (err = %v)", got, tt.code, err)
			}
		})
	}
}

// newCSR returns a DER-encoded, correctly self-signed CSR.
func newCSR(t *testing.T) []byte {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "actor"},
	}, priv)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	return der
}

func mintCertRequest(t *testing.T, actorUID string) *ateapipb.MintCertRequest {
	t.Helper()
	return &ateapipb.MintCertRequest{
		Worker:                    &ateapipb.ObjectRef{Name: testWorkerName},
		ExpectedActorUid:          actorUID,
		CertificateSigningRequest: newCSR(t),
		Purpose:                   ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL,
	}
}

// actorFixture describes the actor/worker pair seeded into the store.
type actorFixture struct {
	state      ateapipb.ActorState
	workerNode string
	// actorWorkerName overrides the Worker the actor points at while leaving
	// the requesting worker unchanged, simulating a stale reciprocal
	// assignment.
	actorWorkerName string
	// assignedTo overrides the actor the worker claims to be hosting. The zero
	// value means the worker is assigned to the seeded actor.
	assignedTo resources.ActorRef
	// unassigned seeds the worker with no assignment at all, as pause, suspend
	// and crash leave it once they have released it.
	unassigned bool
	// noPlacement seeds the actor with no worker assignment.
	noPlacement bool
	// noWorker skips seeding the worker record entirely.
	noWorker bool
	// mismatchedUID simulates a worker assigned to an actor with the same name/atespace but a different UID.
	mismatchedUID bool
}

// seedActor writes an actor, and normally its hosting worker, into st.
func seedActor(t *testing.T, ctx context.Context, st store.Interface, f actorFixture) {
	t.Helper()

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}

	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status:        &ateapipb.ActorStatus{State: f.state},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ate-demo", Name: "counter"},
	}
	if !f.noPlacement {
		workerName := testWorkerName
		if f.actorWorkerName != "" {
			workerName = f.actorWorkerName
		}
		actor.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
			Worker:          &ateapipb.ObjectRef{Name: workerName},
			WorkerNamespace: testPodNS,
			WorkerPool:      testPool,
			WorkerPod:       testWorkerPod,
			WorkerPodUid:    testWorkerPodUID,
		}
	}
	created := storetest.MustCreateActor(t, ctx, st, actor)

	if f.noWorker {
		return
	}
	assigned := f.assignedTo
	if assigned == (resources.ActorRef{}) {
		assigned = actorRef
	}
	assignedActorUID := created.GetMetadata().GetUid()
	if f.mismatchedUID || assigned != actorRef {
		assignedActorUID = "other-actor-uid"
	}
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerName},
		WorkerNamespace: testPodNS,
		WorkerPool:      testPool,
		WorkerPod:       testWorkerPod,
		WorkerPodUid:    testWorkerPodUID,
		NodeName:        f.workerNode,
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
	}
	if _, err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	if f.unassigned {
		return
	}
	bindActor(t, ctx, st, testWorkerName, &ateapipb.ActorAssignment{
		Actor:    assigned.ToObjectRef(),
		ActorUid: assignedActorUID,
	})
}

// bindActor places an actor on a worker at whatever version it is currently at.
func bindActor(t *testing.T, ctx context.Context, st store.Interface, workerName string, assignment *ateapipb.ActorAssignment) {
	t.Helper()
	if err := st.BindActorToWorker(ctx, workerName, assignment, nil); err != nil {
		t.Fatalf("bind actor %s to worker: %v", assignment.GetActorUid(), err)
	}
}

// runningOnNode is the fixture for a healthy actor hosted on nodeName.
func runningOnNode(nodeName string) actorFixture {
	return actorFixture{state: ateapipb.ActorState_ACTOR_STATE_RUNNING, workerNode: nodeName}
}

// TestMintCertAuthorization covers the gate deciding whether a caller may mint
// a certificate for the requested actor.
func TestMintCertAuthorization(t *testing.T) {
	// ptr is needed because "" is itself a case under test, so the zero value
	// cannot double as "use the default".
	ptr := func(s string) *string { return &s }

	for name, tc := range map[string]struct {
		// cert builds the caller's certificate. Nil means a well-formed atelet
		// on the node hosting the actor.
		cert func(t *testing.T) *x509.Certificate
		// noPeer calls the RPC with no transport credentials at all.
		noPeer bool

		fixture actorFixture

		// Request fields override their defaults when non-nil. A nil worker
		// override leaves the request pointing at the seeded worker.
		worker           *ateapipb.ObjectRef
		noWorker         bool
		expectedActorUID *string

		wantCode codes.Code
	}{
		"atelet on the hosting node mints for a running actor": {
			fixture:  runningOnNode(testNode),
			wantCode: codes.OK,
		},
		"actor is in ACTOR_STATE_DELETING with active worker assignment": {
			fixture:  actorFixture{state: ateapipb.ActorState_ACTOR_STATE_DELETING, workerNode: testNode},
			wantCode: codes.FailedPrecondition,
		},
		"caller presented no certificate": {
			noPeer:   true,
			fixture:  runningOnNode(testNode),
			wantCode: codes.Unauthenticated,
		},
		"caller is not the atelet service account": {
			cert: func(t *testing.T) *x509.Certificate {
				id := ateletauthtest.PodIdentityOn(testNode)
				id.ServiceAccountName = "some-workload"
				return ateletauthtest.Cert(t, path.Join("ns", ateletauth.Namespace, "sa", "some-workload"), id)
			},
			fixture:  runningOnNode(testNode),
			wantCode: codes.PermissionDenied,
		},
		"caller is an atelet in the wrong namespace": {
			cert: func(t *testing.T) *x509.Certificate {
				id := ateletauthtest.PodIdentityOn(testNode)
				id.Namespace = "someone-elses-system"
				return ateletauthtest.Cert(t, path.Join("ns", "someone-elses-system", "sa", ateletauth.ServiceAccount), id)
			},
			fixture:  runningOnNode(testNode),
			wantCode: codes.PermissionDenied,
		},
		"certificate carries no SPIFFE URI": {
			cert: func(t *testing.T) *x509.Certificate {
				return ateletauthtest.Cert(t, "", ateletauthtest.PodIdentityOn(testNode))
			},
			fixture:  runningOnNode(testNode),
			wantCode: codes.PermissionDenied,
		},
		"certificate carries no PodIdentity extension": {
			cert: func(t *testing.T) *x509.Certificate {
				return ateletauthtest.Cert(t, path.Join("ns", ateletauth.Namespace, "sa", ateletauth.ServiceAccount), nil)
			},
			fixture:  runningOnNode(testNode),
			wantCode: codes.PermissionDenied,
		},
		"actor does not exist": {
			fixture: actorFixture{
				state:      ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode: testNode,
				assignedTo: resources.ActorRef{Atespace: testAtespace, Name: "no-such-actor"},
			},
			wantCode: codes.PermissionDenied,
		},
		"actor exists under a different atespace": {
			fixture: actorFixture{
				state:      ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode: testNode,
				assignedTo: resources.ActorRef{Atespace: "some-other-atespace", Name: testActorName},
			},
			wantCode: codes.PermissionDenied,
		},
		"actor is hosted on a different node": {
			fixture:  runningOnNode(testOtherNode),
			wantCode: codes.PermissionDenied,
		},
		"worker names a different Pod UID": {
			fixture:  runningOnNode(testNode),
			worker:   &ateapipb.ObjectRef{Name: "9a2f7b81-4c60-4d13-8e5a-3f0b6c8d1e27"},
			wantCode: codes.PermissionDenied,
		},
		"worker is assigned to a different actor": {
			fixture: actorFixture{
				state:      ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode: testNode,
				assignedTo: resources.ActorRef{Atespace: testAtespace, Name: "someone-else"},
			},
			wantCode: codes.PermissionDenied,
		},
		"worker is assigned to an actor with same name and atespace but different UID": {
			fixture: actorFixture{
				state:         ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode:    testNode,
				mismatchedUID: true,
			},
			wantCode: codes.PermissionDenied,
		},
		"actor points to a different worker": {
			fixture: actorFixture{
				state:           ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode:      testNode,
				actorWorkerName: "7c3d9e15-2a48-4b6f-9d01-8e5a3f0b6c8d",
			},
			wantCode: codes.PermissionDenied,
		},
		"hosting worker record is missing": {
			fixture: actorFixture{
				state:      ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode: testNode,
				noWorker:   true,
			},
			wantCode: codes.PermissionDenied,
		},
		"actor has no placement": {
			fixture: actorFixture{
				state:       ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode:  testNode,
				noPlacement: true,
			},
			wantCode: codes.FailedPrecondition,
		},
		"worker has been released": {
			fixture: actorFixture{
				state:      ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode: testNode,
				unassigned: true,
			},
			wantCode: codes.PermissionDenied,
		},
		"worker is unset": {
			fixture:  runningOnNode(testNode),
			noWorker: true,
			wantCode: codes.InvalidArgument,
		},
		"worker name is empty": {
			fixture:  runningOnNode(testNode),
			worker:   &ateapipb.ObjectRef{},
			wantCode: codes.InvalidArgument,
		},
		"worker carries an atespace": {
			fixture:  runningOnNode(testNode),
			worker:   &ateapipb.ObjectRef{Atespace: testAtespace, Name: testWorkerName},
			wantCode: codes.InvalidArgument,
		},
		"expected actor UID is empty": {
			fixture:          runningOnNode(testNode),
			expectedActorUID: ptr(""),
			wantCode:         codes.InvalidArgument,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			seedActor(t, ctx, st, tc.fixture)
			srv := newTestServer(t, st)

			var callerCert *x509.Certificate
			switch {
			case tc.noPeer:
			case tc.cert != nil:
				callerCert = tc.cert(t)
			default:
				callerCert = ateletauthtest.CertOn(t, testNode)
			}

			actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
			if err != nil {
				t.Fatalf("read seeded actor: %v", err)
			}
			req := mintCertRequest(t, actor.GetMetadata().GetUid())
			switch {
			case tc.noWorker:
				req.Worker = nil
			case tc.worker != nil:
				req.Worker = tc.worker
			}
			if tc.expectedActorUID != nil {
				req.ExpectedActorUid = *tc.expectedActorUID
			}
			resp, err := srv.MintCert(ateletauthtest.ContextWith(callerCert), req)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("MintCert() code = %v (err = %v), want %v", got, err, tc.wantCode)
			}
			if tc.wantCode == codes.PermissionDenied {
				// Denials are deliberately indistinguishable (see denyMint): the
				// message must not vary with why the mint was refused, or a
				// caller could probe workers it is not entitled to.
				msg := status.Convert(err).Message()
				if msg != "caller is not permitted" &&
					msg != "caller is not permitted to mint credentials for this actor" {
					t.Errorf("MintCert() denial leaks its reason: %q", msg)
				}
			}
			if tc.wantCode != codes.OK {
				if resp != nil {
					t.Errorf("MintCert() returned a response alongside an error")
				}
				return
			}

			if len(resp.GetActorCertificates()) == 0 {
				t.Fatal("MintCert() returned no certificates")
			}
			leaf, err := x509.ParseCertificate(resp.GetActorCertificates()[0])
			if err != nil {
				t.Fatalf("parse minted certificate: %v", err)
			}
			want := "spiffe://substrate-actor.local/atespace/" + testAtespace + "/actor/" + testActorName
			if len(leaf.URIs) != 1 || leaf.URIs[0].String() != want {
				t.Errorf("minted SPIFFE URI = %v, want %q", leaf.URIs, want)
			}
		})
	}
}

func TestMintCertRejectsUnsupportedPurpose(t *testing.T) {
	server := newTestServer(t, nil)
	for name, purpose := range map[string]ateapipb.ActorCertificatePurpose{
		"unspecified": ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_UNSPECIFIED,
		"unknown":     ateapipb.ActorCertificatePurpose(99),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := server.MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), &ateapipb.MintCertRequest{Purpose: purpose})
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("MintCert() code = %v (err = %v), want %v", got, err, codes.InvalidArgument)
			}
		})
	}
}

// mintCertFor seeds a running actor and mints a certificate for it, returning
// the parsed leaf alongside the UID the store assigned the actor. The request
// is built from that UID, since it is only known once the actor exists.
func mintCertFor(t *testing.T, request func(actorUID string) *ateapipb.MintCertRequest) (*x509.Certificate, string, error) {
	t.Helper()

	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	seedActor(t, ctx, st, runningOnNode(testNode))
	actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
	if err != nil {
		t.Fatalf("read seeded actor: %v", err)
	}
	actorUID := actor.GetMetadata().GetUid()
	if actorUID == "" {
		t.Fatal("seeded actor has no UID; the store is expected to assign one")
	}

	resp, err := newTestServer(t, st).MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), request(actorUID))
	if err != nil {
		return nil, actorUID, err
	}
	if len(resp.GetActorCertificates()) == 0 {
		t.Fatal("MintCert() returned no certificates")
	}
	leaf, err := x509.ParseCertificate(resp.GetActorCertificates()[0])
	if err != nil {
		t.Fatalf("parse minted certificate: %v", err)
	}
	return leaf, actorUID, nil
}

// TestMintCertEmbedsActorIdentity checks that a minted certificate carries the
// ActorIdentity extension, naming the actor the store knows about.
func TestMintCertEmbedsActorIdentity(t *testing.T) {
	leaf, actorUID, err := mintCertFor(t, func(actorUID string) *ateapipb.MintCertRequest {
		return mintCertRequest(t, actorUID)
	})
	if err != nil {
		t.Fatalf("MintCert(): %v", err)
	}

	got, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil {
		t.Fatalf("ActorIdentityFromCertificate: %v", err)
	}
	if got == nil {
		t.Fatal("minted certificate carries no ActorIdentity extension")
	}
	want := &substratex509.ActorIdentity{
		Atespace:  testAtespace,
		ActorName: testActorName,
		ActorUid:  actorUID,
		Purpose:   substratex509.ActorIdentityPurposeAtunnel,
	}
	if *got != *want {
		t.Errorf("ActorIdentity = %+v, want %+v", got, want)
	}
}

// TestMintCertActorUID checks that expected_actor_uid rejects a request that
// crossed an actor reassignment. It never decides the certificate identity,
// which always comes from ateapi state.
func TestMintCertActorUID(t *testing.T) {
	for name, tc := range map[string]struct {
		requestUID func(actorUID string) string
		wantCode   codes.Code
	}{
		"Matching": {requestUID: func(actorUID string) string { return actorUID }, wantCode: codes.OK},
		"Stale":    {requestUID: func(string) string { return "9d1f7b06-3c58-4a2e-8b40-5f7c1e9a2d63" }, wantCode: codes.FailedPrecondition},
	} {
		t.Run(name, func(t *testing.T) {
			leaf, actorUID, err := mintCertFor(t, func(actorUID string) *ateapipb.MintCertRequest {
				req := mintCertRequest(t, actorUID)
				req.ExpectedActorUid = tc.requestUID(actorUID)
				return req
			})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("MintCert() code = %v (err = %v), want %v", got, err, tc.wantCode)
			}
			if tc.wantCode != codes.OK {
				return
			}

			identity, err := substratex509.ActorIdentityFromCertificate(leaf)
			if err != nil {
				t.Fatalf("ActorIdentityFromCertificate: %v", err)
			}
			if identity == nil {
				t.Fatal("minted certificate carries no ActorIdentity extension")
			}
			if identity.ActorUid != actorUID {
				t.Errorf("ActorIdentity.ActorUid = %q, want the stored UID %q", identity.ActorUid, actorUID)
			}
		})
	}
}

// TestMintCertActorState pins down that the actor's state does not gate
// minting: an actor still assigned to a worker on the caller's node gets a
// credential whatever state it carries, except while it is being deleted.
//
// ACTOR_STATE_RESUMING is the case that matters in practice. atelet mints
// while serving the Run/Restore RPC that ateapi issues before marking the
// actor RUNNING, so gating on RUNNING would make every resume unsatisfiable.
//
// The terminal states below are seeded with a worker assignment that the
// control plane would already have cleared, so they are not reachable in a
// healthy system; they are exercised to record that the assignment, not the
// state, is what the decision rests on. Enumerating the enum rather than
// listing states means a state added later is covered without editing this
// test.
func TestMintCertActorState(t *testing.T) {
	for value, name := range ateapipb.ActorState_name {
		actorState := ateapipb.ActorState(value)
		wantCode := codes.OK
		if actorState == ateapipb.ActorState_ACTOR_STATE_DELETING {
			wantCode = codes.FailedPrecondition
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			seedActor(t, ctx, st, actorFixture{state: actorState, workerNode: testNode})
			srv := newTestServer(t, st)

			actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
			if err != nil {
				t.Fatal(err)
			}
			_, err = srv.MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), mintCertRequest(t, actor.GetMetadata().GetUid()))
			if got := status.Code(err); got != wantCode {
				t.Errorf("MintCert() code = %v (err = %v), want %v", got, err, wantCode)
			}
		})
	}
}

// TestMintCertDeniesUnassignedActorWhateverItsState checks that the placement
// checks — not the state — are what stops a departed actor. A RUNNING actor
// whose worker has been released is refused just as a SUSPENDED one is.
func TestMintCertDeniesUnassignedActorWhateverItsState(t *testing.T) {
	for name, actorState := range map[string]ateapipb.ActorState{
		"Running":   ateapipb.ActorState_ACTOR_STATE_RUNNING,
		"Suspended": ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			// The worker still exists on the caller's node but has been released,
			// which is what pause, suspend and crash all do before writing the
			// terminal state.
			seedActor(t, ctx, st, actorFixture{
				state:      actorState,
				workerNode: testNode,
				unassigned: true,
			})
			srv := newTestServer(t, st)

			actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
			if err != nil {
				t.Fatal(err)
			}
			_, err = srv.MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), mintCertRequest(t, actor.GetMetadata().GetUid()))
			if got := status.Code(err); got != codes.PermissionDenied {
				t.Errorf("MintCert() code = %v (err = %v), want %v", got, err, codes.PermissionDenied)
			}
		})
	}
}

// TestMintCertAuthorizesBeforeSigning checks that the gate runs before any CSR
// parsing or CA material is touched. An unauthorized caller must be rejected
// with PermissionDenied even when the rest of the request is unusable, so that
// a failure downstream of the gate can never mask the authorization decision.
func TestMintCertAuthorizesBeforeSigning(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	seedActor(t, ctx, st, runningOnNode(testOtherNode))

	// A server whose CA pool file does not exist: reaching the signing path at
	// all would surface as Internal rather than PermissionDenied.
	workers := workercache.New(st, time.Hour)
	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := workers.Start(cacheCtx); err != nil {
		t.Fatal(err)
	}

	certificateAuthority, err := localca.GenerateCA("test-actor-ca", localca.KeyTypeED25519, 24*time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	certificateAuthorityPool := &localca.ConcretePool{
		CAs:              []*localca.CA{certificateAuthority},
		ActiveForSigning: "test-actor-ca",
	}

	jwtAuthority, err := localjwtauthority.GenerateECDSAP256Authority("1")
	if err != nil {
		t.Fatalf("while generating JWT authority: %v", err)
	}
	jwtAuthorityPool := &localjwtauthority.ConcretePool{
		Authorities:      []*localjwtauthority.Authority{jwtAuthority},
		ActiveForSigning: "1",
	}

	srv := New("issuer", jwtAuthorityPool, certificateAuthorityPool, st, workers)

	actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
	if err != nil {
		t.Fatal(err)
	}
	req := mintCertRequest(t, actor.GetMetadata().GetUid())
	req.CertificateSigningRequest = []byte("not a CSR")
	_, err = srv.MintCert(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), req)
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Errorf("MintCert() code = %v (err = %v), want %v", got, err, codes.PermissionDenied)
	}
}

func TestValidateMintJWTRequest(t *testing.T) {
	// This test verifies validation of user input for minting a JWT.
	validReq := func(mods ...func(req *ateapipb.MintJWTRequest)) *ateapipb.MintJWTRequest {
		req := &ateapipb.MintJWTRequest{
			Audience:  []string{"aud1"},
			Atespace:  "as1",
			ActorName: "actor1",
			ActorUid:  "01234567-89ab-cdef-0123-456789abcdef",
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.MintJWTRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing audience",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Audience = nil }),
		field.ErrorList{field.Required(field.NewPath("audience"), "")},
	}, {
		"too many audiences",
		validReq(func(r *ateapipb.MintJWTRequest) {
			r.Audience = make([]string, 17)
			for i := range r.Audience {
				r.Audience[i] = fmt.Sprintf("https://svc-%d.example.com", i)
			}
		}),
		field.ErrorList{field.TooMany(field.NewPath("audience"), 17, 16).WithOrigin("maxItems")},
	}, {
		"duplicate audience entry",
		validReq(func(r *ateapipb.MintJWTRequest) {
			r.Audience = []string{"https://a.example.com", "https://a.example.com"}
		}),
		field.ErrorList{field.Duplicate(field.NewPath("audience").Index(1), nil)},
	}, {
		"audience entry too long",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Audience = []string{strings.Repeat("a", 513)} }),
		field.ErrorList{field.TooLong(field.NewPath("audience").Index(0), nil, 512).WithOrigin("maxLength")},
	}, {
		"missing atespace",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"invalid atespace",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Atespace = "AS1" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor_name",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorName = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		"invalid actor_name",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorName = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"unspecified actor_uid",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorUid = "" }),
		nil,
	}, {
		"invalid actor_uid",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorUid = "not a uid" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateMintJWTRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateMintCertRequest(t *testing.T) {
	// This test verifies validation of user input for minting a certificate.
	validReq := func(mods ...func(req *ateapipb.MintCertRequest)) *ateapipb.MintCertRequest {
		req := &ateapipb.MintCertRequest{
			Worker:                    &ateapipb.ObjectRef{Name: "worker1"},
			CertificateSigningRequest: []byte{0x01},
			ExpectedActorUid:          "01234567-89ab-cdef-0123-456789abcdef",
			Purpose:                   ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL,
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.MintCertRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"oversized certificate_signing_request",
		validReq(func(r *ateapipb.MintCertRequest) { r.CertificateSigningRequest = make([]byte, 16385) }),
		field.ErrorList{field.TooLong(field.NewPath("certificate_signing_request"), nil, 16384)},
	}, {
		"missing worker",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker = nil }),
		field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		"worker.atespace must be empty",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker.Atespace = "as1" }),
		field.ErrorList{field.Forbidden(field.NewPath("worker", "atespace"), "")},
	}, {
		"missing worker.name",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("worker", "name"), "")},
	}, {
		"invalid worker.name",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker.Name = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("worker", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing certificate_signing_request",
		validReq(func(r *ateapipb.MintCertRequest) { r.CertificateSigningRequest = nil }),
		field.ErrorList{field.Required(field.NewPath("certificate_signing_request"), "")},
	}, {
		"missing expected_actor_uid",
		validReq(func(r *ateapipb.MintCertRequest) { r.ExpectedActorUid = "" }),
		field.ErrorList{field.Required(field.NewPath("expected_actor_uid"), "")},
	}, {
		"invalid expected_actor_uid",
		validReq(func(r *ateapipb.MintCertRequest) { r.ExpectedActorUid = "not a uid" }),
		field.ErrorList{field.Invalid(field.NewPath("expected_actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		"unspecified purpose",
		validReq(func(r *ateapipb.MintCertRequest) {
			r.Purpose = ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_UNSPECIFIED
		}),
		field.ErrorList{field.Required(field.NewPath("purpose"), "")},
	}, {
		"out-of-range purpose",
		validReq(func(r *ateapipb.MintCertRequest) { r.Purpose = ateapipb.ActorCertificatePurpose(99) }),
		field.ErrorList{field.Invalid(field.NewPath("purpose"), nil, "").WithOrigin("maximum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateMintCertRequest(context.Background(), tt.req), tt.want)
		})
	}
}
