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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	testAtespace  = "team-alpha"
	testActorName = "counter-1"
	testPodNS     = "ate-workers"
	testWorkerPod = "worker-abc"
	testPool      = "pool-1"
	testNode      = "node-a"
	testOtherNode = "node-b"
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
func newTestCert(t *testing.T, spiffePath string, podIdentity *substratex509.PodIdentity) *x509.Certificate {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-caller"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	if spiffePath != "" {
		template.URIs = []*url.URL{{Scheme: "spiffe", Host: ateletTrustDomain, Path: spiffePath}}
	}
	if podIdentity != nil {
		if err := substratex509.AddPodIdentityToCertificate(podIdentity, template); err != nil {
			t.Fatalf("add pod identity: %v", err)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert
}

// podIdentityOn returns a well-formed atelet PodIdentity pinned to nodeName.
func podIdentityOn(nodeName string) *substratex509.PodIdentity {
	return &substratex509.PodIdentity{
		Namespace:          ateletNamespace,
		ServiceAccountName: ateletSA,
		ServiceAccountUID:  "sa-uid",
		PodName:            "atelet-xyz",
		PodUID:             "pod-uid",
		NodeName:           nodeName,
		NodeUID:            "node-uid",
	}
}

// ateletCertOn returns the certificate of the atelet running on nodeName.
func ateletCertOn(t *testing.T, nodeName string) *x509.Certificate {
	t.Helper()
	return newTestCert(t, path.Join("ns", ateletNamespace, "sa", ateletSA), podIdentityOn(nodeName))
}

// ctxWithCert injects cert as the transport-authenticated peer certificate.
// A nil cert yields a context with no peer information at all, which is what
// an unauthenticated call looks like.
func ctxWithCert(cert *x509.Certificate) context.Context {
	ctx := context.Background()
	if cert == nil {
		return ctx
	}
	return peer.NewContext(ctx, &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}},
		},
	})
}

// newTestServer returns a Server backed by st, with a freshly generated actor
// CA pool written to a temp file.
func newTestServer(t *testing.T, st store.Interface) *Server {
	t.Helper()

	ca, err := localca.GenerateED25519CA("test-actor-ca")
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	poolBytes, err := localca.Marshal(&localca.Pool{CAs: []*localca.CA{ca}})
	if err != nil {
		t.Fatalf("marshal CA pool: %v", err)
	}
	poolFile := filepath.Join(t.TempDir(), "actor-ca-pool.json")
	if err := os.WriteFile(poolFile, poolBytes, 0o600); err != nil {
		t.Fatalf("write CA pool: %v", err)
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
	return New("issuer", "", poolFile, st, workers)
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
		WorkerNamespace:           testPodNS,
		WorkerPod:                 testWorkerPod,
		WorkerPodUid:              "worker-uid",
		ExpectedActorUid:          actorUID,
		CertificateSigningRequest: newCSR(t),
		Purpose:                   ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL,
	}
}

// actorFixture describes the actor/worker pair seeded into the store.
type actorFixture struct {
	state      ateapipb.ActorState
	workerNode string
	// actorWorkerPod overrides the Pod named by the actor while leaving the
	// requesting worker unchanged, simulating a stale reciprocal assignment.
	actorWorkerPod string
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
		Metadata:               &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status:                 &ateapipb.ActorStatus{State: f.state},
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
	}
	if !f.noPlacement {
		workerPod := testWorkerPod
		if f.actorWorkerPod != "" {
			workerPod = f.actorWorkerPod
		}
		actor.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
			WorkerNamespace: testPodNS,
			WorkerPool:      testPool,
			WorkerPod:       workerPod,
			WorkerPodUid:    "worker-uid",
		}
	}
	created, err := st.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("seed actor: %v", err)
	}

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
		WorkerNamespace: testPodNS,
		WorkerPool:      testPool,
		WorkerPod:       testWorkerPod,
		WorkerPodUid:    "worker-uid",
		NodeName:        f.workerNode,
		State:           ateapipb.Worker_STATE_ACTIVE,
		Assignment: &ateapipb.Assignment{
			Actor:    assigned.ToObjectRef(),
			ActorUid: assignedActorUID,
		},
	}
	if f.unassigned {
		worker.Assignment = nil
	}
	if err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("seed worker: %v", err)
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

		// Request fields override their defaults when non-nil.
		workerNamespace  *string
		workerPod        *string
		workerPodUID     *string
		expectedActorUID *string

		wantCode codes.Code
	}{
		"atelet on the hosting node mints for a running actor": {
			fixture:  runningOnNode(testNode),
			wantCode: codes.OK,
		},
		"caller presented no certificate": {
			noPeer:   true,
			fixture:  runningOnNode(testNode),
			wantCode: codes.Unauthenticated,
		},
		"caller is not the atelet service account": {
			cert: func(t *testing.T) *x509.Certificate {
				id := podIdentityOn(testNode)
				id.ServiceAccountName = "some-workload"
				return newTestCert(t, path.Join("ns", ateletNamespace, "sa", "some-workload"), id)
			},
			fixture:  runningOnNode(testNode),
			wantCode: codes.PermissionDenied,
		},
		"caller is an atelet in the wrong namespace": {
			cert: func(t *testing.T) *x509.Certificate {
				id := podIdentityOn(testNode)
				id.Namespace = "someone-elses-system"
				return newTestCert(t, path.Join("ns", "someone-elses-system", "sa", ateletSA), id)
			},
			fixture:  runningOnNode(testNode),
			wantCode: codes.PermissionDenied,
		},
		"certificate carries no SPIFFE URI": {
			cert: func(t *testing.T) *x509.Certificate {
				return newTestCert(t, "", podIdentityOn(testNode))
			},
			fixture:  runningOnNode(testNode),
			wantCode: codes.PermissionDenied,
		},
		"certificate carries no PodIdentity extension": {
			cert: func(t *testing.T) *x509.Certificate {
				return newTestCert(t, path.Join("ns", ateletNamespace, "sa", ateletSA), nil)
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
		"worker Pod UID does not match": {
			fixture:      runningOnNode(testNode),
			workerPodUID: ptr("sibling-worker-uid"),
			wantCode:     codes.PermissionDenied,
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
				state:          ateapipb.ActorState_ACTOR_STATE_RUNNING,
				workerNode:     testNode,
				actorWorkerPod: "replacement-worker",
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
		"worker namespace is empty": {
			fixture:         runningOnNode(testNode),
			workerNamespace: ptr(""),
			wantCode:        codes.InvalidArgument,
		},
		"worker Pod is empty": {
			fixture:   runningOnNode(testNode),
			workerPod: ptr(""),
			wantCode:  codes.InvalidArgument,
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
				callerCert = ateletCertOn(t, testNode)
			}

			actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
			if err != nil {
				t.Fatalf("read seeded actor: %v", err)
			}
			req := mintCertRequest(t, actor.GetMetadata().GetUid())
			if tc.workerNamespace != nil {
				req.WorkerNamespace = *tc.workerNamespace
			}
			if tc.workerPod != nil {
				req.WorkerPod = *tc.workerPod
			}
			if tc.workerPodUID != nil {
				req.WorkerPodUid = *tc.workerPodUID
			}
			if tc.expectedActorUID != nil {
				req.ExpectedActorUid = *tc.expectedActorUID
			}
			resp, err := srv.MintCert(ctxWithCert(callerCert), req)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("MintCert() code = %v (err = %v), want %v", got, err, tc.wantCode)
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
			_, err := server.MintCert(ctxWithCert(ateletCertOn(t, testNode)), &ateapipb.MintCertRequest{Purpose: purpose})
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

	resp, err := newTestServer(t, st).MintCert(ctxWithCert(ateletCertOn(t, testNode)), request(actorUID))
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
		"Stale":    {requestUID: func(string) string { return "uid-of-a-previous-incarnation" }, wantCode: codes.FailedPrecondition},
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
			_, err = srv.MintCert(ctxWithCert(ateletCertOn(t, testNode)), mintCertRequest(t, actor.GetMetadata().GetUid()))
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
			_, err = srv.MintCert(ctxWithCert(ateletCertOn(t, testNode)), mintCertRequest(t, actor.GetMetadata().GetUid()))
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
	srv := New("issuer", "", filepath.Join(t.TempDir(), "missing.json"), st, workers)

	actor, err := st.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: testActorName})
	if err != nil {
		t.Fatal(err)
	}
	req := mintCertRequest(t, actor.GetMetadata().GetUid())
	req.CertificateSigningRequest = []byte("not a CSR")
	_, err = srv.MintCert(ctxWithCert(ateletCertOn(t, testNode)), req)
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Errorf("MintCert() code = %v (err = %v), want %v", got, err, codes.PermissionDenied)
	}
}
