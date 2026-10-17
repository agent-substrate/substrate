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

package identity

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/clustertrustbundle"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/oidcjwt"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// TestActorJWTVerifiesAgainstServedKeySet checks a minted actor JWT the way a
// relying party does: discover the key set from the token's issuer, then
// verify the signature, issuer, and audience against it.
func TestActorJWTVerifiesAgainstServedKeySet(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := t.Context()
	clients := e2e.GetClients()
	actor := createIdleActor(t, ctx, clients, env["BUCKET_NAME"])

	const audience = "https://actor-jwt-e2e.invalid"
	resp, err := clients.SubstrateAPI.MintActorJwt(ctx, &ateapipb.MintActorJwtRequest{
		Actor:             actor,
		Audiences:         []string{audience},
		ExpirationSeconds: 600,
	})
	if err != nil {
		t.Fatalf("MintActorJwt: %v", err)
	}

	issuer := unverifiedIssuer(t, resp.GetActorJwt())
	if err := actoridjwt.ValidateIssuer(issuer); err != nil {
		t.Fatalf("iss claim: %v", err)
	}
	claims, err := oidcjwt.NewVerifier(issuer, []string{audience}, oidcjwt.HTTPKeySource(issuer, idpHTTPClient(t, ctx))).Verify(ctx, resp.GetActorJwt(), time.Now())
	if err != nil {
		t.Fatalf("verifying the minted JWT against the key set %s publishes: %v", issuer, err)
	}
	if want := fmt.Sprintf("actor/%s/%s", actor.GetAtespace(), actor.GetName()); claims.Subject != want {
		t.Errorf("sub = %q, want %q", claims.Subject, want)
	}
	if want := resp.GetExpireTime().AsTime(); !claims.Expiration.Equal(want) {
		t.Errorf("exp = %v, want the response's expire_time %v", claims.Expiration, want)
	}
}

// createIdleActor creates an actor that never runs. Minting needs only the
// actor's record, so its template skips the golden snapshot and selects no
// workers.
func createIdleActor(t *testing.T, ctx context.Context, clients *e2e.Clients, bucket string) *ateapipb.ObjectRef {
	t.Helper()
	atespace := "actor-jwt-" + strings.ToLower(rand.Text()[:8])
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: atespace}},
	}); err != nil {
		t.Fatalf("CreateAtespace %q: %v", atespace, err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{
			Atespace: &ateapipb.ObjectRef{Name: atespace},
		})
	})

	template := &ateapipb.ObjectRef{Atespace: atespace, Name: "idle"}
	if _, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: template.GetAtespace(), Name: template.GetName()},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:      fmt.Sprintf("gs://%s/ate-e2e-actor-jwt/", bucket),
			GoldenSnapshotConfig: &ateapipb.GoldenSnapshotConfig{Mode: ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
		},
		SandboxConfig:  &ateapipb.SandboxConfig{SandboxClass: "gvisor", ConfigName: "gvisor-default"},
		Containers:     []*ateapipb.Container{{Name: "main", Image: "main@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Command: []string{"/main"}}},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"ate.dev/e2e-actor-jwt": "no-workers"}},
	}}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActorTemplate(context.Background(), &ateapipb.DeleteActorTemplateRequest{ActorTemplate: template})
	})

	actor := &ateapipb.ObjectRef{Atespace: atespace, Name: "minted"}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: actor.GetName()},
		ActorTemplate: template,
	}}); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: actor, AnyState: true})
	})
	return actor
}

// unverifiedIssuer reads the iss claim without checking the signature, as a
// relying party does to pick the issuer to discover.
func unverifiedIssuer(t *testing.T, jwt string) string {
	t.Helper()
	segments := strings.Split(jwt, ".")
	if len(segments) != 3 {
		t.Fatalf("minted JWT has %d segments, want 3", len(segments))
	}
	payload, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		t.Fatalf("decoding JWT payload: %v", err)
	}
	var claims struct {
		Issuer string `json:"iss"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("parsing JWT payload: %v", err)
	}
	return claims.Issuer
}

// idpHTTPClient returns a client that reaches ate-idp-server through a
// port-forward whatever host a URL names, and checks its certificate against
// the servicedns trust bundle and the name its Service gives it.
func idpHTTPClient(t *testing.T, ctx context.Context) *http.Client {
	t.Helper()
	clients := e2e.GetClients()
	config, err := ateclient.LoadKubeConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	service := e2e.ResourceName("idp")
	port, stop, err := portforward.ServicePortForward(ctx, config, clients.K8s, e2e.SystemNamespace(), service, 443)
	if err != nil {
		t.Fatalf("port-forwarding to service %s: %v", service, err)
	}
	t.Cleanup(stop)

	bundles, err := clustertrustbundle.NewClient(clients.K8s, nil)
	if err != nil {
		t.Fatal(err)
	}
	list, err := bundles.List(ctx, metav1.ListOptions{
		FieldSelector: "spec.signerName=servicedns.podcert.ate.dev/identity",
		LabelSelector: "podcert.ate.dev/canarying=live",
	})
	if err != nil {
		t.Fatalf("listing servicedns trust bundles: %v", err)
	}
	roots := x509.NewCertPool()
	for _, bundle := range list.Items {
		roots.AppendCertsFromPEM([]byte(bundle.Spec.TrustBundle))
	}
	if len(list.Items) == 0 {
		t.Fatal("no live servicedns ClusterTrustBundle to verify ate-idp-server's certificate with")
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	var dialer net.Dialer
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
			TLSClientConfig: &tls.Config{
				RootCAs:    roots,
				ServerName: fmt.Sprintf("%s.%s.svc", service, e2e.SystemNamespace()),
			},
		},
	}
}
