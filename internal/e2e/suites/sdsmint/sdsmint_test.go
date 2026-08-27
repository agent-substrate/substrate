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

// Package sdsmint e2e-tests the egress gateway's certificate minter.
//
// sdsmint is an SDS server that mints a leaf certificate on demand for the
// SNI Envoy was asked for. Its unit tests cover the SDS protocol against a fake
// Envoy; what they cannot cover is the part that has actually broken in
// practice -- whether the deployed pod's Envoy, CA pool secret and unix socket
// line up. Every assertion here is made on a certificate that came
// off a real handshake through the real gateway.
//
// Actors do reach the gateway. hack/install-ate.sh applies
// manifests/ate-install/ate-api-server.yaml unconditionally, and that sets
// --egress-gateway-address, so every resume arms the egress redirect
// (prepareActorEgress in cmd/ateom-gvisor/main.go). What that traffic proves is
// that the path carries bytes. It never inspects the certificate it was served,
// because an actor cannot: nothing in the cluster trusts the MITM anchor, so an
// actor speaking TLS through the gateway disables verification and takes
// whatever it is handed. A leaf minted for the wrong name, one that outlives
// its --leaf-cert-ttl by an order of magnitude, one chained to a CA nobody
// installed -- every one of those carries traffic as well as a correct one, and the
// pod stays Running throughout. Those are the assertions below, and no amount
// of actor traffic makes them.
//
// Reaching sdsmint at all now means getting past the front door, which
// authenticates the caller as a named actor: an actor-identity client
// certificate at the TLS layer, and an ext_proc check that the certified actor
// is one the control plane says is running. So the suite creates one actor and
// mints a credential for it (actoridentity_test.go), and the two tests at the
// bottom assert the two ways that door says no. That makes this suite the only
// end-to-end coverage of the egress authorization leg as well as of the minter.
//
// One actor and one probe pod serve the whole suite, because standing them up
// costs more than every handshake here put together and neither is what any
// test is about. Both are torn down with the suite rather than with the test
// that triggered them; see liveActor and sharedProbe.
package sdsmint

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/portforward"
)

const (
	// Where the gateway and its CA live. Both are fixed by
	// manifests/ate-install/atenet-egress.yaml and hack/install-ate.sh.
	egressNamespace = "ate-system"
	mitmCASecret    = "egress-mitm-ca-pool"
	mitmCASecretKey = "pool"
	mitmCAID        = "mitm"

	// leafTTL is --leaf-cert-ttl on the sdsmint sidecar, and leafSkew is the
	// backdating sdsmint/certauth applies to NotBefore. Their sum is the validity
	// span every leaf should carry. Keep both in step with the manifest: a leaf
	// that suddenly lasts hours is the failure this pair is here to catch.
	leafTTL  = 15 * time.Minute
	leafSkew = 5 * time.Minute

	probeName = "egressprobe"
)

// skipUntilPresubmit disables this suite. Every test here needs the sdsmint
// gateway deployed, which no presubmit lane installs.
//
// TODO(haiyanmeng): enable the test after we figure out how to test them
// through presubmit.
func skipUntilPresubmit(t *testing.T) {
	t.Helper()
	t.Skip("needs an sdsmint egress gateway that no presubmit lane deploys yet")
}

// TestSdsmintMintsALeafPerSNI is the core functional assertion, and the only
// test here that exercises the minter rather than the door in front of it:
// several SNIs through one gateway come back as that many distinct
// certificates, each issued for the name that was asked for, each chaining
// directly to the MITM CA, each short-lived.
//
// Two of the names are ordinary and three deliberately are not, which is what
// pins "mint for every name" as deployed: the gateway mints for names nobody
// enumerated, at depths and in TLDs no allowlist pattern would have covered.
// This test used to assert the opposite -- that an SNI outside an allowlist
// was refused -- and its inversion is the whole content of that change. If the
// gateway is ever given an allowlist again, this test failing is the intended
// signal, not a regression: restore the refusal assertion rather than
// narrowing the names below.
//
// Every SNI is qualified with the probe's namespace, which is fresh per run, so
// each is a name Envoy has never subscribed to. Reusing one would be served
// from Envoy's live secret set and the test would pass without sdsmint having
// minted anything.
func TestSdsmintMintsALeafPerSNI(t *testing.T) {
	skipUntilPresubmit(t)

	ctx := context.Background()

	root := mitmRootCertificate(t, ctx)
	probe := sharedProbe(t, ctx)

	// Nothing has to resolve: the mint happens during the inner handshake,
	// before the gateway looks for an upstream. .invalid can never be delegated
	// (RFC 6761), and neither the depth nor the TLD of the last three was
	// reachable under the old "example.com *.example.com" allowlist, so a
	// half-reverted config cannot pass this by accident.
	snis := []string{
		probe.uniqueSNI("a.example.com"),
		probe.uniqueSNI("b.example.com"),
		probe.uniqueSNI("notallowed.invalid"),       // a TLD no pattern mentioned
		probe.uniqueSNI("a.b.deep.example.com"),     // deeper than one "*" label
		probe.uniqueSNI("UPPER.Notallowed.Invalid"), // and case-insensitively
	}

	serials := map[string]string{}
	for _, sni := range snis {
		result := probe.handshake(t, ctx, sni)
		if !result.OK {
			t.Errorf("gateway refused to mint for %q at stage %q, so it is not minting for every name: %s", sni, result.Stage, result.Error)
			continue
		}
		chain := parseChain(t, sni, result.ChainPEM)
		leaf := chain[0]

		// Minted for the name that was asked for, and only that name. A leaf
		// carrying anything else means the SNI Envoy policed is not the name
		// the certificate authorizes. Case-insensitively, because a ClientHello
		// SNI is a DNS name and Envoy is free to normalize it.
		if got := leaf.DNSNames; len(got) != 1 || !strings.EqualFold(got[0], sni) {
			t.Errorf("leaf for %q has DNSNames %v, want exactly [%q]", sni, got, sni)
		}
		// And carrying no subject: the SAN above is the whole of the leaf's
		// identity, so there is no second name for the two to disagree on.
		if got := leaf.Subject.String(); got != "" {
			t.Errorf("leaf for %q has subject %q, want empty", sni, got)
		}

		// Chains to the MITM root the installer created, verified for the SNI
		// itself so the root's dNSName constraint is applied too.
		opts := x509.VerifyOptions{
			DNSName:       strings.ToLower(sni),
			Roots:         certPool(root),
			Intermediates: certPool(chain[1:]...),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		if _, err := leaf.Verify(opts); err != nil {
			t.Errorf("leaf for %q does not verify against the %s/%s MITM root: %v", sni, egressNamespace, mitmCASecret, err)
		}

		// Short-lived, which is what bounds the damage from a leaked leaf key
		// and what makes the MITM CA tolerable at all. The window is generous
		// because the point is to catch a TTL that is wrong by an order of
		// magnitude, not clock skew.
		validity := leaf.NotAfter.Sub(leaf.NotBefore)
		if want := leafTTL + leafSkew; validity < want-time.Minute || validity > want+time.Minute {
			t.Errorf("leaf for %q is valid for %v, want about %v (--leaf-cert-ttl in atenet-egress.yaml)", sni, validity, want)
		}
		if time.Now().After(leaf.NotAfter) {
			t.Errorf("leaf for %q was already expired when served (NotAfter %s)", sni, leaf.NotAfter)
		}

		// One certificate per name, not one certificate reused under many
		// names. Serials are compared across the whole set rather than pairwise
		// so a minter that caches on something other than the SNI is caught
		// wherever the collision happens.
		serial := leaf.SerialNumber.Text(16)
		if other, dup := serials[serial]; dup {
			t.Errorf("%q and %q were served the same certificate (serial %s); the gateway is not minting per name", other, sni, serial)
		}
		serials[serial] = sni

		// Signed by the root itself, and the chain carries nothing but the leaf
		// and that root. sdsmint has no delegated-intermediate mode any more,
		// so an extra certificate in the middle means the deployed image is
		// not the one this suite describes.
		if len(chain) != 2 {
			t.Errorf("chain for %q has %d certificates, want 2 (leaf + root)", sni, len(chain))
			continue
		}
		if !chain[1].Equal(root) {
			t.Errorf("leaf for %q is not chained to the %s/%s root; issuer is %q",
				sni, egressNamespace, mitmCASecret, chain[1].Subject)
		}

		t.Logf("%s: served a %d-cert chain, leaf serial %s valid %v, issued by %q constrained to %v",
			sni, len(chain), serial, validity, root.Subject.CommonName, root.PermittedDNSDomains)
	}
}

// TestGatewayRefusesANonActorWorkload is the front door's half of the egress
// story: the gateway trusts only the actor-identity CA, so a credential that
// proves "some substrate workload" no longer opens a tunnel. The probe's own
// podidentity certificate is exactly that credential, and it is the one this
// suite presented before actors had to authenticate at all.
//
// The refusal has to come from the gateway's mTLS specifically. A probe that
// failed to dial, or that got as far as the CONNECT, would be reporting
// something other than the front door turning it away.
func TestGatewayRefusesANonActorWorkload(t *testing.T) {
	skipUntilPresubmit(t)

	ctx := context.Background()

	probe := sharedProbe(t, ctx)

	sni := probe.uniqueSNI("podidentity.example.com")
	result := probe.handshakeAs(t, ctx, sni, podIdentityCredentialPath)
	if result.OK {
		t.Fatalf("the gateway opened a tunnel for the probe's podidentity credential; its front door is accepting workloads that are not actors")
	}
	if result.Stage != stageGatewayTLS {
		t.Fatalf("podidentity credential failed at stage %q, want %q -- the probe was stopped by something other than the front door, so this test is not checking it: %s",
			result.Stage, stageGatewayTLS, result.Error)
	}
	t.Logf("gateway refused the non-actor credential at its front door as expected: %s", result.Error)
}

// TestGatewayRefusesAnUnknownActor covers the check that only ext_proc can
// make. The credential here is cryptographically perfect -- signed by the real
// actor-identity CA, correct extension, correct purpose -- so Envoy completes
// the handshake, and the CONNECT is denied only because the control plane has
// no such actor. Without this, nothing distinguishes a gateway that authorizes
// on the certificate alone from one that authorizes on control-plane state, and
// the difference is whether a deleted actor's credential still works.
func TestGatewayRefusesAnUnknownActor(t *testing.T) {
	skipUntilPresubmit(t)

	ctx := context.Background()

	probe := sharedProbe(t, ctx)

	sni := probe.uniqueSNI("unknown.example.com")
	result := probe.handshakeAs(t, ctx, sni, unknownActorCredentialPath)
	if result.OK {
		t.Fatalf("the gateway tunneled for an actor the control plane has never heard of; the ext_proc identity check is not running")
	}
	// A 403 on the CONNECT, not a TLS failure: the certificate was accepted and
	// the identity it carries was rejected. A failure at stageGatewayTLS would
	// mean the request never reached ext_proc, and the denial would prove
	// nothing about the control-plane lookup.
	if result.Stage != stageConnect || result.ConnectStatus != http.StatusForbidden {
		t.Fatalf("unknown actor was refused at stage %q with CONNECT status %d, want %q and %d -- something other than the ext_proc identity check turned it away: %s",
			result.Stage, result.ConnectStatus, stageConnect, http.StatusForbidden, result.Error)
	}
	t.Logf("gateway denied the unknown actor at CONNECT as expected: %s", result.Error)
}

// mitmRootCertificate reads the trust anchor sdsmint signs under, straight
// from the secret the sidecar mounts, so the test is checking the chain against
// the CA that is actually deployed rather than one it was told about.
func mitmRootCertificate(t *testing.T, ctx context.Context) *x509.Certificate {
	t.Helper()
	secret, err := e2e.GetClients().K8s.CoreV1().Secrets(egressNamespace).Get(ctx, mitmCASecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading MITM CA pool secret %s/%s: %v", egressNamespace, mitmCASecret, err)
	}
	// This unmarshals the signing key along with the certificate. That is
	// unavoidable -- the pool is one blob -- and acceptable only because this
	// runs against a test cluster with a kubeconfig that could read the secret
	// anyway. Nothing below touches the key.
	pool, err := localca.Unmarshal(secret.Data[mitmCASecretKey])
	if err != nil {
		t.Fatalf("parsing MITM CA pool from %s/%s key %q: %v", egressNamespace, mitmCASecret, mitmCASecretKey, err)
	}
	for _, ca := range pool.CAs {
		if ca.ID == mitmCAID {
			return ca.RootCertificate
		}
	}
	t.Fatalf("MITM CA pool %s/%s has no CA with id %q", egressNamespace, mitmCASecret, mitmCAID)
	return nil
}

// probeClient talks to the in-cluster egress probe over a port-forward.
type probeClient struct {
	// ns is the probe's namespace, randomized per run. Tests qualify their
	// SNIs with it so that no name is one Envoy already holds a secret for.
	ns      string
	baseURL string
	http    *http.Client
}

// uniqueSNI qualifies suffix with the probe's namespace, producing a name no
// earlier run has asked the gateway for. Nothing withdraws a secret any more,
// so Envoy holds every name it has ever been given for the life of its process:
// an SNI a previous run used comes back from its secret set without sdsmint
// minting anything, and the test passes having tested nothing.
func (c *probeClient) uniqueSNI(suffix string) string {
	return c.ns + "-" + suffix
}

var (
	probeOnce sync.Once
	probeVal  *probeClient
	probeErr  error
)

// sharedProbe returns the one probe pod the whole suite uses.
func sharedProbe(t *testing.T, ctx context.Context) *probeClient {
	t.Helper()
	probeOnce.Do(func() {
		// startProbe reports failures through t, which unwinds this goroutine
		// without returning. Leave something behind so the tests that run
		// afterwards fail pointing at the first one instead of dereferencing
		// nil.
		defer func() {
			if probeVal == nil && probeErr == nil {
				probeErr = errors.New("setup did not complete; see the failure reported by the first test that needed the probe")
			}
		}()
		probeVal = startProbe(t, ctx)
	})
	if probeErr != nil {
		t.Fatalf("starting the shared egress probe: %v", probeErr)
	}
	return probeVal
}

// startProbe creates the probe's namespace, mints its credentials there, builds
// and deploys the probe, waits for it to be ready, and returns a client for it.
func startProbe(t *testing.T, ctx context.Context) *probeClient {
	t.Helper()
	if _, err := e2e.CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ns := e2e.CreateNamespace(t).Name

	provisionProbeCredentials(t, ctx, ns)
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	tmpl, err := os.ReadFile(filepath.Join(root, "internal/e2e/fixtures/egressprobe/egressprobe.yaml.tmpl"))
	if err != nil {
		t.Fatalf("reading egressprobe manifest template: %v", err)
	}
	manifest := filepath.Join(t.TempDir(), "egressprobe.yaml")
	rendered := strings.ReplaceAll(string(tmpl), "${NAMESPACE}", ns)
	if err := os.WriteFile(manifest, []byte(rendered), 0o644); err != nil {
		t.Fatalf("writing rendered egressprobe manifest: %v", err)
	}

	applyArgs := []string{"ko", "apply", "-f", manifest}
	if e2e.KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+e2e.KubeContext)
	}
	e2e.RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	waitForProbeReady(t, ctx, ns)

	config, err := ateclient.LoadConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("creating k8s client for port-forward: %v", err)
	}
	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, ns, probeName, 8080)
	if err != nil {
		t.Fatalf("port-forwarding %s/%s: %v", ns, probeName, err)
	}
	e2e.RegisterSuiteCleanup(stop)

	return &probeClient{
		ns:      ns,
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:    &http.Client{Timeout: 90 * time.Second},
	}
}

func waitForProbeReady(t *testing.T, ctx context.Context, ns string) {
	t.Helper()
	const timeout = 3 * time.Minute
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		pod, err := e2e.GetClients().K8s.CoreV1().Pods(ns).Get(ctx, probeName, metav1.GetOptions{})
		switch {
		case err != nil:
			lastState = err.Error()
		case portforward.IsPodReady(pod):
			t.Logf("probe pod %s/%s is ready", ns, probeName)
			return
		default:
			lastState = describeProbeState(pod)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out after %v waiting for probe pod %s/%s to become ready: %s", timeout, ns, probeName, lastState)
}

func describeProbeState(pod *corev1.Pod) string {
	parts := []string{"phase=" + string(pod.Status.Phase)}
	for _, cs := range pod.Status.ContainerStatuses {
		switch {
		case cs.State.Waiting != nil:
			parts = append(parts, fmt.Sprintf("%s waiting: %s: %s", cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message))
		case cs.State.Terminated != nil:
			parts = append(parts, fmt.Sprintf("%s terminated: %s: %s", cs.Name, cs.State.Terminated.Reason, cs.State.Terminated.Message))
		default:
			parts = append(parts, fmt.Sprintf("%s running, ready=%t", cs.Name, cs.Ready))
		}
	}
	return strings.Join(parts, "; ")
}

// handshake asks the probe to complete one inner TLS handshake for sni,
// presenting the actor credential the suite minted. A refused SNI comes back as
// a result with OK false, not as an error: refusal is one of the outcomes under
// test.
func (c *probeClient) handshake(t *testing.T, ctx context.Context, sni string) handshakeResult {
	t.Helper()
	return c.handshakeAs(t, ctx, sni, "")
}

// handshakeAs is handshake with a credential other than the probe's default.
// An empty credential means the default.
func (c *probeClient) handshakeAs(t *testing.T, ctx context.Context, sni, credential string) handshakeResult {
	t.Helper()
	endpoint := c.baseURL + "/handshake?sni=" + url.QueryEscape(sni)
	if credential != "" {
		endpoint += "&credential-bundle=" + url.QueryEscape(credential)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("building probe request for %q: %v", sni, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("calling probe for %q: %v", sni, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		t.Fatalf("probe returned %d for %q: %s", resp.StatusCode, sni, strings.TrimSpace(string(body)))
	}
	var out handshakeResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding probe response for %q: %v", sni, err)
	}
	return out
}

// Stages a handshake can fail at, mirroring the probe's stage constants. Only
// the two the tests assert on are named here; the rest arrive as whatever
// string the probe sent and are printed in the failure.
const (
	stageGatewayTLS = "gateway_tls"
	stageConnect    = "connect"
)

// handshakeResult mirrors the probe's response body. It is duplicated rather
// than imported because the probe is package main.
type handshakeResult struct {
	SNI        string `json:"sni"`
	Credential string `json:"credential"`
	OK         bool   `json:"ok"`
	// Stage is where a failed handshake stopped. Asserting on it rather than
	// on Error is what keeps "the front door refused the certificate" and "the
	// door opened and ext_proc said no" from being the same test: they are
	// different hops, and their messages are only incidentally different.
	Stage string `json:"stage"`
	// ConnectStatus is the status the gateway answered the CONNECT with, set
	// only when Stage is stageConnect.
	ConnectStatus int    `json:"connect_status"`
	Error         string `json:"error"`
	ChainPEM      string `json:"chain_pem"`
}

func parseChain(t *testing.T, sni, chainPEM string) []*x509.Certificate {
	t.Helper()
	var chain []*x509.Certificate
	rest := []byte(chainPEM)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("parsing certificate served for %q: %v", sni, err)
		}
		chain = append(chain, cert)
	}
	if len(chain) == 0 {
		t.Fatalf("no certificates in the chain served for %q", sni)
	}
	return chain
}

func certPool(certs ...*x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, cert := range certs {
		pool.AddCert(cert)
	}
	return pool
}
