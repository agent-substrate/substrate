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
	"github.com/agent-substrate/substrate/internal/localca"
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

	return New("issuer", "audience", "", poolFile, "", nil, st)
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

// actorFixture describes the actor/worker pair seeded into the store.
type actorFixture struct {
	status     ateapipb.Actor_Status
	workerNode string
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
}

// seedActor writes an actor, and normally its hosting worker, into st.
func seedActor(t *testing.T, ctx context.Context, st store.Interface, f actorFixture) {
	t.Helper()

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}
	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status:                 f.status,
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
	}
	if !f.noPlacement {
		actor.WorkerAssignment = &ateapipb.WorkerAssignment{
			WorkerNamespace: testPodNS,
			WorkerPool:      testPool,
			WorkerPod:       testWorkerPod,
			WorkerPodUid:    "worker-uid",
		}
	}
	if _, err := st.CreateActor(ctx, actor); err != nil {
		t.Fatalf("seed actor: %v", err)
	}

	if f.noWorker {
		return
	}
	assigned := f.assignedTo
	if assigned == (resources.ActorRef{}) {
		assigned = actorRef
	}
	worker := &ateapipb.Worker{
		WorkerNamespace: testPodNS,
		WorkerPool:      testPool,
		WorkerPod:       testWorkerPod,
		WorkerPodUid:    "worker-uid",
		NodeName:        f.workerNode,
		State:           ateapipb.Worker_STATE_ACTIVE,
		Assignment:      &ateapipb.Assignment{Actor: assigned.ToObjectRef()},
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
	return actorFixture{status: ateapipb.Actor_STATUS_RUNNING, workerNode: nodeName}
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

		// atespace and actorName override the request fields when non-nil.
		atespace  *string
		actorName *string

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
			fixture:   runningOnNode(testNode),
			actorName: ptr("no-such-actor"),
			wantCode:  codes.PermissionDenied,
		},
		"actor exists under a different atespace": {
			fixture:  runningOnNode(testNode),
			atespace: ptr("some-other-atespace"),
			wantCode: codes.PermissionDenied,
		},
		"actor is hosted on a different node": {
			fixture:  runningOnNode(testOtherNode),
			wantCode: codes.PermissionDenied,
		},
		"worker is assigned to a different actor": {
			fixture: actorFixture{
				status:     ateapipb.Actor_STATUS_RUNNING,
				workerNode: testNode,
				assignedTo: resources.ActorRef{Atespace: testAtespace, Name: "someone-else"},
			},
			wantCode: codes.PermissionDenied,
		},
		"hosting worker record is missing": {
			fixture: actorFixture{
				status:     ateapipb.Actor_STATUS_RUNNING,
				workerNode: testNode,
				noWorker:   true,
			},
			wantCode: codes.PermissionDenied,
		},
		"actor has no placement": {
			fixture: actorFixture{
				status:      ateapipb.Actor_STATUS_RUNNING,
				noPlacement: true,
				noWorker:    true,
			},
			wantCode: codes.FailedPrecondition,
		},
		"worker has been released": {
			fixture: actorFixture{
				status:     ateapipb.Actor_STATUS_RUNNING,
				workerNode: testNode,
				unassigned: true,
			},
			wantCode: codes.PermissionDenied,
		},
		"atespace is empty": {
			fixture:  runningOnNode(testNode),
			atespace: ptr(""),
			wantCode: codes.InvalidArgument,
		},
		"actor name is empty": {
			fixture:   runningOnNode(testNode),
			actorName: ptr(""),
			wantCode:  codes.InvalidArgument,
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

			atespace, actorName := testAtespace, testActorName
			if tc.atespace != nil {
				atespace = *tc.atespace
			}
			if tc.actorName != nil {
				actorName = *tc.actorName
			}

			resp, err := srv.MintCert(ctxWithCert(callerCert), &ateapipb.MintCertRequest{
				Atespace:                  atespace,
				ActorName:                 actorName,
				CertificateSigningRequest: newCSR(t),
			})
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
	leaf, actorUID, err := mintCertFor(t, func(string) *ateapipb.MintCertRequest {
		return &ateapipb.MintCertRequest{
			Atespace:                  testAtespace,
			ActorName:                 testActorName,
			CertificateSigningRequest: newCSR(t),
		}
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
	}
	if *got != *want {
		t.Errorf("ActorIdentity = %+v, want %+v", got, want)
	}
}

// TestMintCertActorUID checks how the caller-supplied actor_uid is treated: it
// is honored when it agrees with the store, ignored when absent, and refused
// when it names some other incarnation of the actor. In no case does it decide
// what goes into the certificate — that always comes from the store.
func TestMintCertActorUID(t *testing.T) {
	for name, tc := range map[string]struct {
		// requestUID derives the actor_uid the caller sends from the UID the
		// store assigned the seeded actor.
		requestUID func(actorUID string) string
		wantCode   codes.Code
	}{
		"Omitted":  {requestUID: func(string) string { return "" }, wantCode: codes.OK},
		"Matching": {requestUID: func(actorUID string) string { return actorUID }, wantCode: codes.OK},
		"Stale":    {requestUID: func(string) string { return "uid-of-a-previous-incarnation" }, wantCode: codes.PermissionDenied},
	} {
		t.Run(name, func(t *testing.T) {
			leaf, actorUID, err := mintCertFor(t, func(actorUID string) *ateapipb.MintCertRequest {
				return &ateapipb.MintCertRequest{
					Atespace:                  testAtespace,
					ActorName:                 testActorName,
					ActorUid:                  tc.requestUID(actorUID),
					CertificateSigningRequest: newCSR(t),
				}
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

// TestMintCertActorStatus pins down that the actor's status does not gate
// minting: an actor still assigned to a worker on the caller's node gets a
// credential whatever status it carries, except while it is being deleted.
//
// STATUS_RESUMING is the case that matters in practice. atelet mints while
// serving the Run/Restore RPC that ateapi issues before marking the actor
// RUNNING, so gating on RUNNING would make every resume unsatisfiable.
//
// The terminal statuses below are seeded with a worker assignment that the
// control plane would already have cleared, so they are not reachable in a
// healthy system; they are exercised to record that the assignment, not the
// status, is what the decision rests on. Enumerating the enum rather than
// listing statuses means a status added later is covered without editing this
// test.
func TestMintCertActorStatus(t *testing.T) {
	for value, name := range ateapipb.Actor_Status_name {
		actorStatus := ateapipb.Actor_Status(value)
		wantCode := codes.OK
		if actorStatus == ateapipb.Actor_STATUS_DELETING {
			wantCode = codes.FailedPrecondition
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			seedActor(t, ctx, st, actorFixture{status: actorStatus, workerNode: testNode})
			srv := newTestServer(t, st)

			_, err := srv.MintCert(ctxWithCert(ateletCertOn(t, testNode)), &ateapipb.MintCertRequest{
				Atespace:                  testAtespace,
				ActorName:                 testActorName,
				CertificateSigningRequest: newCSR(t),
			})
			if got := status.Code(err); got != wantCode {
				t.Errorf("MintCert() code = %v (err = %v), want %v", got, err, wantCode)
			}
		})
	}
}

// TestMintCertDeniesUnassignedActorWhateverItsStatus checks that the placement
// checks — not the status — are what stops a departed actor. A RUNNING actor
// whose worker has been released is refused just as a SUSPENDED one is.
func TestMintCertDeniesUnassignedActorWhateverItsStatus(t *testing.T) {
	for name, actorStatus := range map[string]ateapipb.Actor_Status{
		"Running":   ateapipb.Actor_STATUS_RUNNING,
		"Suspended": ateapipb.Actor_STATUS_SUSPENDED,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			// The worker still exists on the caller's node but has been released,
			// which is what pause, suspend and crash all do before writing the
			// terminal status.
			seedActor(t, ctx, st, actorFixture{
				status:     actorStatus,
				workerNode: testNode,
				unassigned: true,
			})
			srv := newTestServer(t, st)

			_, err := srv.MintCert(ctxWithCert(ateletCertOn(t, testNode)), &ateapipb.MintCertRequest{
				Atespace:                  testAtespace,
				ActorName:                 testActorName,
				CertificateSigningRequest: newCSR(t),
			})
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
	srv := New("issuer", "audience", "", filepath.Join(t.TempDir(), "missing.json"), "", nil, st)

	_, err := srv.MintCert(ctxWithCert(ateletCertOn(t, testNode)), &ateapipb.MintCertRequest{
		Atespace:                  testAtespace,
		ActorName:                 testActorName,
		CertificateSigningRequest: []byte("not a CSR"),
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Errorf("MintCert() code = %v (err = %v), want %v", got, err, codes.PermissionDenied)
	}
}
