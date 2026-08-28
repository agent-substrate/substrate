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

package egressauthz

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net/url"
	"path"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

// The gateway's front door requires a client certificate signed by the
// actor-identity CA, and its ext_proc sidecar then looks the certified actor up
// in the control plane. Getting through it therefore needs both halves: a leaf
// this pool signs, and an actor the ate API agrees is running. The suite mints
// the first half and never supplies the second, which is the whole of
// TestGatewayRefusesAnUnknownActor.
const (
	actorIDCASecret    = "actor-id-ca-pool"
	actorIDCASecretKey = "pool"

	// The atespace named in the credential below. No such atespace is created
	// -- nothing looks it up until ext_proc does, and ext_proc is meant to come
	// back empty-handed.
	probeAtespace = "ate-egressauthz-e2e"

	// actorCertificateLifetime matches what ateapi's MintCert issues. Nothing
	// here depends on the exact value -- the suite runs in minutes -- but a
	// credential that outlives the real one would hide an expiry bug in the
	// gateway rather than reproduce it.
	actorCertificateLifetime = time.Hour

	// Where the probe pod finds the credential the suite mints for it. Kept in
	// step with testserver/egressprobe.yaml.tmpl.
	unknownActorCredentialSecret = "egressprobe-unknown-actor"
	unknownActorCredentialPath   = "/run/actor-identity-unknown/credential-bundle.pem"

	credentialBundleKey = "credential-bundle.pem"

	// podIdentityCredentialPath is the probe's own workload identity: a valid
	// substrate credential that is not an actor. It is mounted so the suite can
	// show the gateway refusing it.
	podIdentityCredentialPath = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
)

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

// provisionProbeCredentials mints the one credential the probe pod mounts: a
// leaf the real actor-identity CA signed, naming an actor the control plane has
// never heard of.
func provisionProbeCredentials(t *testing.T, ctx context.Context, ns string) {
	t.Helper()

	// The name is scoped to the probe's namespace so a stray record cannot
	// collide with anything.
	writeCredentialSecret(t, ctx, ns, unknownActorCredentialSecret, mintActorCredential(t, actorIdentityCA(t, ctx), &substratex509.ActorIdentity{
		Atespace:  probeAtespace,
		ActorName: "no-such-actor-" + ns,
		ActorUid:  "00000000-0000-0000-0000-000000000000",
		Purpose:   substratex509.ActorIdentityPurposeAtunnel,
	}))
}
