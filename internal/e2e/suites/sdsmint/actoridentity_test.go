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

package sdsmint

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// The gateway's front door requires a client certificate signed by the
// actor-identity CA, and its ext_proc sidecar then looks the certified actor up
// in the control plane. Getting through it therefore needs both halves: a leaf
// this pool signs, and an actor the ate API agrees is running.
const (
	actorIDCASecret    = "actor-id-ca-pool"
	actorIDCASecretKey = "pool"

	// The atespace the suite's actor lives in. Fixed rather than randomized so
	// a leaked actor from an aborted run is easy to find and delete.
	probeAtespace = "ate-sdsmint-e2e"

	// actorCertificateLifetime matches what ateapi's MintCert issues. Nothing
	// here depends on the exact value -- the suite runs in minutes -- but a
	// credential that outlives the real one would hide an expiry bug in the
	// gateway rather than reproduce it.
	actorCertificateLifetime = time.Hour

	// Where the probe pod finds the credentials the suite mints for it. Kept in
	// step with egressprobe.yaml.tmpl and the --credential-bundle default in
	// the probe.
	actorCredentialSecret = "egressprobe-actor-identity"

	unknownActorCredentialSecret = "egressprobe-unknown-actor"
	unknownActorCredentialPath   = "/run/actor-identity-unknown/credential-bundle.pem"

	credentialBundleKey = "credential-bundle.pem"

	// podIdentityCredentialPath is the probe's own workload identity: a valid
	// substrate credential that is not an actor. It is mounted so the suite can
	// show the gateway refusing it.
	podIdentityCredentialPath = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
)

// templateRef is the ActorTemplate the suite's actor is created from. The
// default is the one every other suite uses; the environment overrides exist
// for clusters that install the fixtures elsewhere.
func templateRef() (namespace, name string) {
	namespace, name = "ate-demo-counter", "counter"
	if v := os.Getenv("E2E_TEMPLATE_NAMESPACE"); v != "" {
		namespace = v
	}
	if v := os.Getenv("E2E_TEMPLATE_NAME"); v != "" {
		name = v
	}
	return namespace, name
}

// probeActor is the identity the probe authenticates to the gateway as.
type probeActor struct {
	atespace string
	name     string
	uid      string
}

func (a *probeActor) identity() *substratex509.ActorIdentity {
	return &substratex509.ActorIdentity{
		Atespace:  a.atespace,
		ActorName: a.name,
		ActorUid:  a.uid,
		Purpose:   substratex509.ActorIdentityPurposeAtunnel,
	}
}

// liveActor returns the one actor the whole suite shares.
var liveActor = sync.OnceValues(createLiveActor)

func createLiveActor() (*probeActor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	clients := e2e.GetClients()
	tmplNS, tmplName := templateRef()
	name := fmt.Sprintf("sdsmint-probe-%d", time.Now().UnixNano())
	ref := &ateapipb.ObjectRef{Atespace: probeAtespace, Name: name}

	// CreateActor requires the atespace to exist first; a second run finds it
	// already there, which is not an error.
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: probeAtespace}},
	})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: probeAtespace, Name: name},
		ActorTemplateNamespace: tmplNS,
		ActorTemplateName:      tmplName,
	}}); err != nil {
		return nil, fmt.Errorf("creating actor %s/%s from template %s/%s: %w (install the fixture with hack/install-demo-counter.sh, or point E2E_TEMPLATE_NAMESPACE/E2E_TEMPLATE_NAME at another one)",
			probeAtespace, name, tmplNS, tmplName, err)
	}
	e2e.RegisterSuiteCleanup(func() {
		// DeleteActor requires the actor to be suspended first. Both are
		// best-effort: a run that could not reach the API here has already
		// failed louder somewhere else.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: ref})
	})

	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref, Boot: true}); err != nil {
		return nil, fmt.Errorf("resuming actor %s/%s: %w", probeAtespace, name, err)
	}

	deadline := time.Now().Add(4 * time.Minute)
	var lastStatus ateapipb.Actor_Status
	for time.Now().Before(deadline) {
		actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
		if err == nil {
			lastStatus = actor.GetStatus()
			if lastStatus == ateapipb.Actor_STATUS_RUNNING {
				uid := actor.GetMetadata().GetUid()
				if uid == "" {
					return nil, fmt.Errorf("actor %s/%s is running but has no UID", probeAtespace, name)
				}
				return &probeActor{atespace: probeAtespace, name: name, uid: uid}, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for actor %s/%s to run: %w", probeAtespace, name, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("actor %s/%s never reached STATUS_RUNNING (last status %v); a saturated worker pool is the usual cause",
		probeAtespace, name, lastStatus)
}

// actorIdentityCA returns the CA that signs actor certificates, straight from
// the secret ateapi signs with.
func actorIdentityCA(t *testing.T, ctx context.Context) *localca.CA {
	t.Helper()
	secret, err := e2e.GetClients().K8s.CoreV1().Secrets(egressNamespace).Get(ctx, actorIDCASecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading actor-identity CA pool secret %s/%s: %v", egressNamespace, actorIDCASecret, err)
	}
	pool, err := localca.Unmarshal(secret.Data[actorIDCASecretKey])
	if err != nil {
		t.Fatalf("parsing actor-identity CA pool from %s/%s key %q: %v", egressNamespace, actorIDCASecret, actorIDCASecretKey, err)
	}
	if len(pool.CAs) == 0 {
		t.Fatalf("actor-identity CA pool %s/%s contains no CA", egressNamespace, actorIDCASecret)
	}
	// CAs[0] is the one that signs: ateapi's MintCert makes the same choice.
	return pool.CAs[0]
}

// mintActorCredential issues a client credential for identity, in the shape
// atunnel gets from ateapi.
func mintActorCredential(t *testing.T, ca *localca.CA, identity *substratex509.ActorIdentity) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating actor key: %v", err)
	}

	template := &x509.Certificate{
		URIs: []*url.URL{{
			Scheme: "spiffe",
			Host:   "substrate-actor.local",
			Path:   path.Join("atespace", identity.Atespace, "actor", identity.ActorName),
		}},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(actorCertificateLifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		Issuer:                pkix.Name{CommonName: "api.ate-system.svc.cluster.local"},
	}
	if err := substratex509.AddActorIdentityToCertificate(identity, template); err != nil {
		t.Fatalf("adding the ActorIdentity extension for %s/%s: %v", identity.Atespace, identity.ActorName, err)
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca.RootCertificate, key.Public(), ca.SigningKey)
	if err != nil {
		t.Fatalf("signing the actor certificate for %s/%s: %v", identity.Atespace, identity.ActorName, err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling the actor key: %v", err)
	}

	// A credential bundle as internal/credbundle parses it: the PKCS#8 key
	// first, then the chain leaf-first.
	bundle := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	for _, intermediate := range ca.IntermediateCertificates {
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: intermediate.Raw})...)
	}
	return bundle
}

// writeCredentialSecret puts a minted bundle where the probe pod can mount it.
func writeCredentialSecret(t *testing.T, ctx context.Context, ns, name string, bundle []byte) {
	t.Helper()
	_, err := e2e.GetClients().K8s.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{credentialBundleKey: bundle},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating credential secret %s/%s: %v", ns, name, err)
	}
}

// provisionProbeCredentials mints everything the probe pod mounts: the
// credential of the live actor, and one for an actor that does not exist. Both
// are signed by the real CA, so the difference between them is exactly the
// control-plane check and nothing else. Which one a handshake presents is
// chosen per request, which is what lets the whole suite share one pod.
func provisionProbeCredentials(t *testing.T, ctx context.Context, ns string) *probeActor {
	t.Helper()

	actor, err := liveActor()
	if err != nil {
		t.Fatalf("preparing the actor the probe authenticates as: %v", err)
	}
	t.Logf("probe authenticates as actor %s/%s (uid %s)", actor.atespace, actor.name, actor.uid)

	ca := actorIdentityCA(t, ctx)
	writeCredentialSecret(t, ctx, ns, actorCredentialSecret, mintActorCredential(t, ca, actor.identity()))

	// Same CA, same shape, an actor the control plane has never heard of. The
	// name is scoped to the probe's namespace so a stray record cannot collide
	// with anything.
	writeCredentialSecret(t, ctx, ns, unknownActorCredentialSecret, mintActorCredential(t, ca, &substratex509.ActorIdentity{
		Atespace:  probeAtespace,
		ActorName: "no-such-actor-" + ns,
		ActorUid:  "00000000-0000-0000-0000-000000000000",
		Purpose:   substratex509.ActorIdentityPurposeAtunnel,
	}))

	return actor
}
