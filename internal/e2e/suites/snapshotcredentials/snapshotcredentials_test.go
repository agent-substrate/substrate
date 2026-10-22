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

// Package snapshotcredentials proves, against real GCS, that the credential
// exchange hands a node a token that reaches exactly the actor's own snapshot
// prefixes. It runs only on a GCS install. The kind lane stores snapshots in
// an S3-compatible service, where the exchange answers UNIMPLEMENTED, so the
// suite skips there, or fails when E2E_REQUIRE_GCS says a GCS lane was meant.
package snapshotcredentials

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/objectstoreplugin/exchange"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/internal/preview"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

const probeTemplate = "probe"

// requireGCS skips, or fails under e2e.GCSRequired, unless the install under
// test stores snapshots in GCS. The backend is the objectstore-plugin
// sidecar's ATE_STORAGE_BACKEND on ate-api-server.
func requireGCS(t *testing.T, ctx context.Context, clients *e2e.Clients) {
	t.Helper()
	deploy, err := clients.K8s.AppsV1().Deployments(e2e.SystemNamespace()).Get(ctx, e2e.ResourceName("ate-api-server"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the ate-api-server Deployment: %v", err)
	}
	backend := "gcs"
	for _, c := range deploy.Spec.Template.Spec.InitContainers {
		if c.Name != "objectstore-plugin" {
			continue
		}
		for _, env := range c.Env {
			if env.Name == "ATE_STORAGE_BACKEND" && env.Value != "" {
				backend = env.Value
			}
		}
	}
	e2e.RequireGCSBackend(t, backend)
}

// signer mints worker-for-actor JWTs with the install's own pool, as
// ate-api-server would for atelet. The harness cannot call
// MintWorkerActorJwt, which needs atelet's identity, so it reads the pool
// Secret with its admin client and signs with the same code and keys.
type signer struct {
	pool   *localjwtauthority.ConcretePool
	issuer string
}

func newSigner(t *testing.T, ctx context.Context, clients *e2e.Clients) *signer {
	t.Helper()
	ns := e2e.SystemNamespace()
	secret, err := clients.K8s.CoreV1().Secrets(ns).Get(ctx, e2e.ResourceName("actor-id-jwt-pool"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the actor JWT pool Secret: %v", err)
	}
	pool, err := localjwtauthority.Unmarshal(secret.Data["pool"])
	if err != nil {
		t.Fatalf("decoding the actor JWT pool: %v", err)
	}
	cm, err := clients.K8s.CoreV1().ConfigMaps(ns).Get(ctx, e2e.ResourceName("ate-api-server-envvars"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the ate-api-server ConfigMap: %v", err)
	}
	issuer := cm.Data["ATE_API_ACTOR_JWT_ISSUER"]
	if issuer == "" {
		t.Fatal("ATE_API_ACTOR_JWT_ISSUER is empty in the ate-api-server ConfigMap")
	}
	return &signer{pool: pool, issuer: issuer}
}

type claims struct {
	kind     actoridjwt.Kind
	audience string
	actor    *ateapipb.Actor
	uid      string
	node     string
}

// forActor is the token ate-api-server would mint for the node hosting actor.
func forActor(actor *ateapipb.Actor) claims {
	return claims{
		kind: actoridjwt.KindWorkerForActor, audience: exchange.ActorJWTAudience,
		actor: actor, uid: actor.GetMetadata().GetUid(), node: actor.GetStatus().GetAssignedNode(),
	}
}

func (s *signer) jwt(t *testing.T, c claims) string {
	t.Helper()
	now := time.Now()
	meta := c.actor.GetMetadata()
	token, err := s.pool.SignJWT(&actoridjwt.Claims{
		Kind:       c.kind,
		Issuer:     s.issuer,
		Subject:    c.kind.SubjectPrefix() + "/" + meta.GetAtespace() + "/" + meta.GetName(),
		Audiences:  []string{c.audience},
		Expiration: now.Add(time.Hour),
		NotBefore:  now.Add(-time.Minute),
		IssuedAt:   now,
		Substrate:  actoridjwt.SubstrateClaims{Atespace: meta.GetAtespace(), ActorName: meta.GetName(), ActorUID: c.uid, Node: c.node},
	})
	if err != nil {
		t.Fatalf("signing a %v token: %v", c.kind, err)
	}
	return token
}

// dialExchange port-forwards the ate-api-server Service's exchange port and
// verifies it the way the node plugin does, with the service DNS trust bundle
// and the Service's DNS name.
func dialExchange(t *testing.T, ctx context.Context, clients *e2e.Clients) exchangepb.CredentialExchangeClient {
	t.Helper()
	config, err := ateclient.LoadKubeConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	ns := e2e.SystemNamespace()
	port, stop, err := portforward.ServicePortForward(ctx, config, clients.K8s, ns, e2e.ResourceName("api"), exchange.DefaultPort)
	if err != nil {
		t.Fatalf("port-forwarding the exchange: %v", err)
	}
	t.Cleanup(stop)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(e2e.ServiceDNSTrustBundle(t, ctx, clients))) {
		t.Fatal("the service DNS trust bundle holds no certificate")
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: e2e.ResourceName("api") + "." + ns + ".svc"})
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("dialing the exchange: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return exchangepb.NewCredentialExchangeClient(conn)
}

// gcs sends one request to GCS's XML API with the downscoped token and
// returns the status code. uri is gs://bucket/object.
func gcs(t *testing.T, ctx context.Context, token, method, uri, body string) int {
	t.Helper()
	path := strings.TrimPrefix(uri, "gs://")
	req, err := http.NewRequestWithContext(ctx, method, "https://storage.googleapis.com/"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, uri, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func expectGCS(t *testing.T, ctx context.Context, what, token, method, uri, body string, want int) {
	t.Helper()
	if got := gcs(t, ctx, token, method, uri, body); got != want {
		t.Errorf("%s: %s %s = %d, want %d", what, method, uri, got, want)
	}
}

// durableURI is the prefix of the actor's generation with the given owner,
// which must be COMPLETED in object storage.
func durableURI(t *testing.T, actor *ateapipb.Actor, owner ateapipb.SnapshotOwner) string {
	t.Helper()
	for _, snap := range actor.GetStatus().GetSnapshots() {
		if snap.GetOwner() == owner && snap.GetDurableSnapshot().GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
			return snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
		}
	}
	t.Fatalf("actor %s has no completed %v snapshot: %v", actor.GetMetadata().GetName(), owner, actor.GetStatus().GetSnapshots())
	return ""
}

func getActor(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) *ateapipb.Actor {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor %s: %v", name, err)
	}
	return actor
}

func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string, want ateapipb.ActorState) *ateapipb.Actor {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		actor := getActor(t, ctx, clients, atespace, name)
		if actor.GetStatus().GetState() == want {
			return actor
		}
		if time.Now().After(deadline) {
			t.Fatalf("actor %s is %v after two minutes, want %v", name, actor.GetStatus().GetState(), want)
		}
		time.Sleep(time.Second)
	}
}

// createAndResume creates an actor from the probe template and resumes it
// from the golden snapshot. The record outlives the fixture namespace, so a
// leftover from a failed run is cleared first.
func createAndResume(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) *ateapipb.Actor {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: probeTemplate},
	}}); err != nil {
		t.Fatalf("CreateActor %s: %v", name, err)
	}
	t.Cleanup(func() {
		// The test's context is cancelled by the time cleanup runs, and an
		// actor left running crashes when the fixture's worker pool goes.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
		if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
			t.Logf("cleanup: DeleteActor %s failed, actor leaked (remove with: kubectl ate delete actor %s -a %s): %v", name, name, atespace, err)
		}
	})
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %s: %v", name, err)
	}
	return waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
}

// suspendSync suspends the actor and waits for its snapshot to be durable.
func suspendSync(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) *ateapipb.Actor {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}}); err != nil {
		t.Fatalf("SuspendActor %s: %v", name, err)
	}
	return waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
}

// TestExchangeScopesTheTokenToTheActor walks the exchange and the boundary
// it grants through one actor's life: fresh from a golden, it may read the
// golden and write only under its own prefix. Once it and a sibling have
// their own snapshots, it reads its own and not the sibling's. Tokens the
// exchange must refuse are checked alongside.
func TestExchangeScopesTheTokenToTheActor(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	clients := e2e.GetClients()
	requireGCS(t, ctx, clients)

	atespace, _ := e2e.DeployProbe(t, env["BUCKET_NAME"], "snapcreds")
	sign := newSigner(t, ctx, clients)
	xchg := dialExchange(t, ctx, clients)
	stamp := time.Now().UTC().Format("20060102T150405Z")

	// Both actors start from the golden snapshot, on whichever nodes the
	// scheduler picks.
	alpha := createAndResume(t, ctx, clients, atespace, "creds-alpha")
	beta := createAndResume(t, ctx, clients, atespace, "creds-beta")
	goldenURI := durableURI(t, alpha, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG)

	// A token for alpha, as its node would present it.
	resp, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, forActor(alpha))})
	if err != nil {
		t.Fatalf("exchange for a running actor: %v", err)
	}
	token := resp.GetAccessToken()
	if left := time.Until(resp.GetExpireTime().AsTime()); token == "" || left <= 0 {
		t.Fatalf("exchange returned token %q expiring in %v", token, left)
	}
	// A forced exchange mints from the current source token, which is
	// replaced once less than 40 minutes are left. A minute is allowed for
	// clock skew between this machine and Google.
	fresh, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, forActor(alpha)), Force: true})
	if err != nil {
		t.Fatalf("forced exchange for a running actor: %v", err)
	}
	if left := time.Until(fresh.GetExpireTime().AsTime()); left < 39*time.Minute {
		t.Errorf("a freshly minted token expires in %v, want at least 39m", left)
	}
	// The golden is readable and not writable. The actor's own prefix,
	// which holds nothing yet, is writable.
	expectGCS(t, ctx, "golden manifest read", token, http.MethodGet, goldenURI+"/manifest.json", "", http.StatusOK)
	expectGCS(t, ctx, "golden write refused", token, http.MethodPut, goldenURI+"/e2e-"+stamp, "x", http.StatusForbidden)
	own := ownPrefix(goldenURI, alpha)
	expectGCS(t, ctx, "own prefix write", token, http.MethodPut, own+"/e2e-"+stamp, "hello", http.StatusOK)
	expectGCS(t, ctx, "own prefix read back", token, http.MethodGet, own+"/e2e-"+stamp, "", http.StatusOK)
	expectGCS(t, ctx, "own prefix delete", token, http.MethodDelete, own+"/e2e-"+stamp, "", http.StatusNoContent)
	expectGCS(t, ctx, "sibling actor's prefix refused", token, http.MethodPut, ownPrefix(goldenURI, beta)+"/e2e-"+stamp, "x", http.StatusForbidden)
	// GCS answers 403 outside the boundary and 404 for a missing object inside
	// it, so the pair below tells a denied read from an absent object.
	expectGCS(t, ctx, "sibling actor's prefix read refused", token, http.MethodGet, ownPrefix(goldenURI, beta)+"/e2e-"+stamp, "", http.StatusForbidden)
	expectGCS(t, ctx, "own prefix read of a missing object", token, http.MethodGet, own+"/e2e-missing-"+stamp, "", http.StatusNotFound)

	// Tokens the exchange refuses, with the code the node plugin would see.
	for _, tc := range []struct {
		name string
		jwt  string
		want codes.Code
	}{
		{"the actor's own token", sign.jwt(t, claims{kind: actoridjwt.KindActor, audience: exchange.ActorJWTAudience, actor: alpha, uid: alpha.GetMetadata().GetUid()}), codes.PermissionDenied},
		{"another audience", sign.jwt(t, withAudience(forActor(alpha), "other")), codes.Unauthenticated},
		{"a recreated actor's old uid", sign.jwt(t, withUID(forActor(alpha), "00000000-0000-4000-8000-000000000000")), codes.NotFound},
		{"a node the actor is not on", sign.jwt(t, withNode(forActor(alpha), "not-the-node")), codes.PermissionDenied},
		{"an empty token", "", codes.InvalidArgument},
	} {
		_, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: tc.jwt})
		if status.Code(err) != tc.want {
			t.Errorf("exchange of %s = %v (%v), want %v", tc.name, status.Code(err), err, tc.want)
		}
	}

	// Each actor gets its own snapshot. Alpha comes back, beta stays
	// suspended with no node, so no node may act for it.
	suspendSync(t, ctx, clients, atespace, "creds-alpha")
	beta = suspendSync(t, ctx, clients, atespace, "creds-beta")
	if _, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, withNode(forActor(beta), "any-node"))}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("exchange for a suspended actor held by no node = %v, want PermissionDenied", err)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: "creds-alpha"}}); err != nil {
		t.Fatalf("ResumeActor creds-alpha: %v", err)
	}
	alpha = waitForActorState(t, ctx, clients, atespace, "creds-alpha", ateapipb.ActorState_ACTOR_STATE_RUNNING)

	// The forced exchange re-derives the grants: the golden is gone from
	// alpha's status, its own snapshot is in.
	resp, err = xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, forActor(alpha)), Force: true})
	if err != nil {
		t.Fatalf("forced exchange after the first suspend: %v", err)
	}
	token = resp.GetAccessToken()
	expectGCS(t, ctx, "own snapshot manifest read", token, http.MethodGet, durableURI(t, alpha, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR)+"/manifest.json", "", http.StatusOK)
	expectGCS(t, ctx, "sibling snapshot manifest refused", token, http.MethodGet, durableURI(t, beta, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR)+"/manifest.json", "", http.StatusForbidden)
	expectGCS(t, ctx, "golden read dropped with the tag generation", token, http.MethodGet, goldenURI+"/manifest.json", "", http.StatusForbidden)
}

// ownPrefix is the actor's own prefix at the location its golden lives in,
// the read-write grant the exchange derives before the actor has a snapshot
// of its own: <location>/atespaces/<atespace>/actors/<uid>.
func ownPrefix(goldenURI string, actor *ateapipb.Actor) string {
	location, _, _ := strings.Cut(goldenURI, "/atespaces/")
	return location + "/atespaces/" + actor.GetMetadata().GetAtespace() + "/actors/" + actor.GetMetadata().GetUid()
}

// locationOf is the storage location a snapshot URI lives under, the part
// before its owner's atespaces segment.
func locationOf(snapshotURI string) string {
	location, _, _ := strings.Cut(snapshotURI, "/atespaces/")
	return location
}

// ownPrefixAt is ownPrefix for a location given directly, for a template
// whose location differs from the golden's.
func ownPrefixAt(location string, actor *ateapipb.Actor) string {
	return strings.TrimSuffix(location, "/") + "/atespaces/" + actor.GetMetadata().GetAtespace() + "/actors/" + actor.GetMetadata().GetUid()
}

// TestRepointMovesTheGrant repoints an actor to a template whose snapshots
// live under another location and checks the exchange follows. Grants come
// from the URIs in the actor's status, so nothing names the new location
// until the first suspend records a pending snapshot there: until then the
// exchange keeps answering for the old location, the suspend's upload is the
// first request the new location sees, and it goes through on the node
// plugin's forced re-exchange. After it, an exchange without force mints for
// the new location and the old token is good for nothing the actor owns.
// Per-template locations are a preview feature, so the test needs the
// install to enable the gate and the run to pass --preview.
func TestRepointMovesTheGrant(t *testing.T) {
	e2e.RequirePreview(t, preview.GatePerTemplateSnapshotLocation)
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	clients := e2e.GetClients()
	requireGCS(t, ctx, clients)

	atespace, probe := e2e.DeployProbe(t, env["BUCKET_NAME"], "snapcreds-repoint")
	sign := newSigner(t, ctx, clients)
	xchg := dialExchange(t, ctx, clients)
	stamp := time.Now().UTC().Format("20060102T150405Z")

	// Template B is the probe template under another location in the same
	// bucket. Its golden is not what the actor resumes from, but the actor's
	// next snapshot goes under B's location.
	const nameB = "probe-repoint"
	locationB := "gs://" + env["BUCKET_NAME"] + "/ate-e2e-repoint-" + stamp + "/"
	templateB := proto.Clone(probe).(*ateapipb.ActorTemplate)
	templateB.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: nameB}
	templateB.Status = nil
	templateB.SnapshotConfig = proto.Clone(probe.GetSnapshotConfig()).(*ateapipb.SnapshotConfig)
	templateB.SnapshotConfig.StorageLocation = locationB
	if _, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: templateB}); err != nil {
		t.Fatalf("CreateActorTemplate %s: %v", nameB, err)
	}
	t.Cleanup(func() {
		if _, err := clients.SubstrateAPI.DeleteActorTemplate(context.Background(), &ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: nameB}}); err != nil {
			t.Logf("cleanup: DeleteActorTemplate %s failed: %v", nameB, err)
		}
	})
	e2e.WaitForSubstrateTemplateReady(ctx, t, clients, atespace, nameB)

	// Running from template A, the token writes under A's location only.
	const name = "creds-repoint"
	actor := createAndResume(t, ctx, clients, atespace, name)
	locationA := locationOf(durableURI(t, actor, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG))
	first, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, forActor(actor))})
	if err != nil {
		t.Fatalf("exchange before the repoint: %v", err)
	}
	expectGCS(t, ctx, "old location write before the repoint", first.GetAccessToken(), http.MethodPut, ownPrefixAt(locationA, actor)+"/e2e-"+stamp, "a", http.StatusOK)
	expectGCS(t, ctx, "old location delete before the repoint", first.GetAccessToken(), http.MethodDelete, ownPrefixAt(locationA, actor)+"/e2e-"+stamp, "", http.StatusNoContent)
	expectGCS(t, ctx, "new location write before the repoint", first.GetAccessToken(), http.MethodPut, ownPrefixAt(locationB, actor)+"/e2e-"+stamp, "b", http.StatusForbidden)

	// A repoint needs a suspended actor that owns no snapshot, so the actor
	// is reverted to its golden rather than suspended.
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	if _, err := clients.SubstrateAPI.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: ref}); err != nil {
		t.Fatalf("RevertActor %s: %v", name, err)
	}
	actor = waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	actor.ActorTemplate = &ateapipb.ObjectRef{Atespace: atespace, Name: nameB}
	if _, err := clients.SubstrateAPI.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: actor}); err != nil {
		t.Fatalf("UpdateActor %s to template %s: %v", name, nameB, err)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %s after the repoint: %v", name, err)
	}
	actor = waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	// The actor's status still names only A's location, so the exchange
	// still grants A and not B, whatever it serves from its cache.
	between, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, forActor(actor))})
	if err != nil {
		t.Fatalf("exchange after the repoint: %v", err)
	}
	expectGCS(t, ctx, "new location write after the repoint, before any snapshot there", between.GetAccessToken(), http.MethodPut, ownPrefixAt(locationB, actor)+"/e2e-"+stamp, "b", http.StatusForbidden)
	expectGCS(t, ctx, "old location write after the repoint", between.GetAccessToken(), http.MethodPut, ownPrefixAt(locationA, actor)+"/e2e-"+stamp, "a", http.StatusOK)
	expectGCS(t, ctx, "old location delete after the repoint", between.GetAccessToken(), http.MethodDelete, ownPrefixAt(locationA, actor)+"/e2e-"+stamp, "", http.StatusNoContent)

	// The real path: the suspend records a pending snapshot under B, atelet
	// asks the node plugin to upload there, GCS refuses the token the node
	// cached for the resume, and one forced re-exchange, now granting B,
	// carries the upload. The snapshot lands under B's location.
	actor = suspendSync(t, ctx, clients, atespace, name)
	snapshot := durableURI(t, actor, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR)
	if !strings.HasPrefix(snapshot, ownPrefixAt(locationB, actor)) {
		t.Fatalf("snapshot after the repoint is at %s, want it under %s", snapshot, ownPrefixAt(locationB, actor))
	}

	// A suspended actor is on no node, so the exchange answers for it only
	// once it runs again. Now the status names B, so an exchange without
	// force must not serve the old token, whose grants no longer cover the
	// actor.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %s after the suspend: %v", name, err)
	}
	actor = waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	after, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, forActor(actor))})
	if err != nil {
		t.Fatalf("exchange after the suspend: %v", err)
	}
	if after.GetAccessToken() == first.GetAccessToken() {
		t.Error("the exchange served the old token after the suspend, its grants no longer cover the actor")
	}
	expectGCS(t, ctx, "snapshot read with the new token", after.GetAccessToken(), http.MethodGet, snapshot+"/manifest.json", "", http.StatusOK)
	expectGCS(t, ctx, "snapshot read with the old token", first.GetAccessToken(), http.MethodGet, snapshot+"/manifest.json", "", http.StatusForbidden)

	// The suspend pruned the actor's generation at A, so A is no longer
	// granted. Only a forced exchange shows that: without force the exchange
	// may serve any cached token that covers B, and the token minted for the
	// upload, while both generations were in status, covers A as well.
	fresh, err := xchg.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: sign.jwt(t, forActor(actor)), Force: true})
	if err != nil {
		t.Fatalf("forced exchange after the suspend: %v", err)
	}
	expectGCS(t, ctx, "old location write after the suspend", fresh.GetAccessToken(), http.MethodPut, ownPrefixAt(locationA, actor)+"/e2e-"+stamp, "a", http.StatusForbidden)
}

func withAudience(c claims, audience string) claims { c.audience = audience; return c }
func withUID(c claims, uid string) claims           { c.uid = uid; return c }
func withNode(c claims, node string) claims         { c.node = node; return c }
