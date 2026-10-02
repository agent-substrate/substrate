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

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// IngressAuthModeEnv names the ingress router's --ingress-auth-mode the
// suites run against: IngressAuthDeprecatedInsecure or IngressAuthStaticMTLS.
// It has to match how the router was installed (ATE_INGRESS_AUTH_MODE for
// ate-setup); unset means deprecated-insecure.
const IngressAuthModeEnv = "E2E_INGRESS_AUTH_MODE"

const (
	IngressAuthDeprecatedInsecure = "deprecated-insecure"
	IngressAuthStaticMTLS         = "static-mtls"
)

// IngressAuthMode returns the router authentication mode the suites expect.
func IngressAuthMode() string {
	if mode := os.Getenv(IngressAuthModeEnv); mode != "" {
		return mode
	}
	return IngressAuthDeprecatedInsecure
}

// IngressAuthModeRequired reports whether IngressAuthModeEnv must be set
// explicitly. In CI an unset mode would quietly skip the ingress
// authentication suite, so it is an error there; REQUIRE_INGRESS_AUTH_MODE
// makes it one locally too.
func IngressAuthModeRequired() bool {
	return os.Getenv("CI") == "true" || os.Getenv("REQUIRE_INGRESS_AUTH_MODE") == "true"
}

// IngressAuthModeSet reports whether IngressAuthModeEnv is set.
func IngressAuthModeSet() bool { return os.Getenv(IngressAuthModeEnv) != "" }

// IngressMTLS reports whether the router requires mTLS client certificates,
// and so whether RouterClient dials its TLS listeners as RouterClientIdentity.
func IngressMTLS() bool { return IngressAuthMode() == IngressAuthStaticMTLS }

// The router client's identity. The router's base manifest
// (manifests/ate-install/atenet-router.yaml) allowlists
// RouterClientServiceAccount's SPIFFE ID, so the two must change together. The
// namespace is fixed rather than per-test because the allowlist is fixed at
// install time.
const (
	RouterClientNamespace      = "ate-e2e-router-client"
	RouterClientServiceAccount = "router-client"
)

// RouterClientSPIFFEID is the SPIFFE ID the podidentity signer issues for
// serviceAccount in RouterClientNamespace.
func RouterClientSPIFFEID(serviceAccount string) string {
	return installdefaults.SPIFFEID(RouterClientNamespace, serviceAccount)
}

const (
	// podIdentitySignerName issues the SPIFFE client certificates the router
	// verifies.
	podIdentitySignerName = "podidentity.podcert.ate.dev/identity"
	// routerClientCertTimeout bounds the wait for the signer to issue.
	routerClientCertTimeout = 2 * time.Minute
)

var (
	routerClientCertsMu sync.Mutex
	routerClientCerts   = map[string]tls.Certificate{}
)

// RouterClientCertificate returns a certificate the podidentity signer issued
// for serviceAccount in RouterClientNamespace. Each ServiceAccount's
// certificate is requested once per test process, and lasts longer than any
// suite.
func RouterClientCertificate(t *testing.T, ctx context.Context, serviceAccount string) tls.Certificate {
	t.Helper()
	routerClientCertsMu.Lock()
	defer routerClientCertsMu.Unlock()
	if cert, ok := routerClientCerts[serviceAccount]; ok {
		return cert
	}

	clientset := GetClients().K8s
	if err := ensureRouterClientNamespace(ctx, clientset); err != nil {
		t.Fatalf("preparing namespace %s: %v", RouterClientNamespace, err)
	}
	cert, err := requestPodIdentityCertificate(ctx, clientset, RouterClientNamespace, serviceAccount, routerClientCertTimeout)
	if err != nil {
		t.Fatalf("requesting a podidentity certificate for %s/%s: %v", RouterClientNamespace, serviceAccount, err)
	}
	routerClientCerts[serviceAccount] = cert
	return cert
}

// requestPodIdentityCertificate creates a PodCertificateRequest for
// serviceAccount in namespace, waits for the podidentity signer to issue it,
// and deletes it again. The key never leaves this process.
//
// The pod, ServiceAccount and node the request names do not exist:
// kube-apiserver only checks them for requests a kubelet creates, and the
// signer takes the identity from the request.
func requestPodIdentityCertificate(ctx context.Context, clientset kubernetes.Interface, namespace, serviceAccount string, timeout time.Duration) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generating key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("creating stub CSR: %w", err)
	}

	// Unique per call: suites run as concurrent processes and share the
	// namespace. rand.Text is upper-case base32; object names are lower case.
	name := "e2e-" + serviceAccount + "-" + strings.ToLower(rand.Text()[:8])
	pcr := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:           podIdentitySignerName,
			PodName:              name,
			PodUID:               uuid.NewUUID(),
			ServiceAccountName:   serviceAccount,
			ServiceAccountUID:    uuid.NewUUID(),
			NodeName:             "e2e-test-process",
			NodeUID:              uuid.NewUUID(),
			MaxExpirationSeconds: ptr.To(int32(24 * 60 * 60)),
			StubPKCS10Request:    csr,
		},
	}
	pcrs, err := podCertificateRequests(clientset, namespace)
	if err != nil {
		return tls.Certificate{}, err
	}
	if err := pcrs.create(ctx, pcr); err != nil {
		return tls.Certificate{}, fmt.Errorf("creating PodCertificateRequest %s/%s: %w", namespace, name, err)
	}
	defer func() {
		// A leftover request is harmless: kube-apiserver garbage collects
		// old PodCertificateRequests.
		delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := pcrs.delete(delCtx, name); err != nil && !apierrors.IsNotFound(err) {
			fmt.Fprintf(os.Stderr, "Failed to delete PodCertificateRequest %s/%s: %v\n", namespace, name, err)
		}
	}()

	var chainPEM string
	err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		status, err := pcrs.status(ctx, name)
		if err != nil {
			return false, err
		}
		for _, c := range status.Conditions {
			if c.Status != metav1.ConditionTrue {
				continue
			}
			switch c.Type {
			case certsv1.PodCertificateRequestConditionTypeIssued:
				chainPEM = status.CertificateChain
				return true, nil
			case certsv1.PodCertificateRequestConditionTypeDenied, certsv1.PodCertificateRequestConditionTypeFailed:
				return false, fmt.Errorf("%s: %s: %s", c.Type, c.Reason, c.Message)
			}
		}
		return false, nil
	})
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("waiting for PodCertificateRequest %s/%s to be issued: %w", namespace, name, err)
	}

	cert := tls.Certificate{PrivateKey: key}
	for rest := []byte(chainPEM); ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		cert.Certificate = append(cert.Certificate, block.Bytes)
	}
	if len(cert.Certificate) == 0 {
		return tls.Certificate{}, fmt.Errorf("PodCertificateRequest %s/%s was issued with no certificates", namespace, name)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parsing the issued leaf certificate: %w", err)
	}
	if !key.PublicKey.Equal(leaf.PublicKey) {
		return tls.Certificate{}, fmt.Errorf("the issued certificate does not carry the requested public key")
	}
	if want := installdefaults.SPIFFEID(namespace, serviceAccount); len(leaf.URIs) != 1 || leaf.URIs[0].String() != want {
		return tls.Certificate{}, fmt.Errorf("the podidentity signer issued a certificate with URI SANs %v, want exactly %s", leaf.URIs, want)
	}
	cert.Leaf = leaf
	return cert, nil
}

// pcrClient creates, reads and deletes PodCertificateRequests through
// whichever of certificates.k8s.io v1 and v1beta1 the cluster serves,
// preferring v1, as the podcertificate controller does.
type pcrClient struct {
	clientset kubernetes.Interface
	namespace string
	v1        bool
}

func podCertificateRequests(clientset kubernetes.Interface, namespace string) (*pcrClient, error) {
	for _, version := range []string{"v1", "v1beta1"} {
		resources, err := clientset.Discovery().ServerResourcesForGroupVersion("certificates.k8s.io/" + version)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("discovering PodCertificateRequest %s: %w", version, err)
		}
		for _, r := range resources.APIResources {
			if r.Name == "podcertificaterequests" {
				return &pcrClient{clientset: clientset, namespace: namespace, v1: version == "v1"}, nil
			}
		}
	}
	return nil, fmt.Errorf("the cluster serves neither v1 nor v1beta1 PodCertificateRequest")
}

func (c *pcrClient) create(ctx context.Context, pcr *certsv1.PodCertificateRequest) error {
	if c.v1 {
		_, err := c.clientset.CertificatesV1().PodCertificateRequests(c.namespace).Create(ctx, pcr, metav1.CreateOptions{})
		return err
	}
	beta := &certsv1beta1.PodCertificateRequest{
		ObjectMeta: pcr.ObjectMeta,
		Spec: certsv1beta1.PodCertificateRequestSpec{
			SignerName:           pcr.Spec.SignerName,
			PodName:              pcr.Spec.PodName,
			PodUID:               pcr.Spec.PodUID,
			ServiceAccountName:   pcr.Spec.ServiceAccountName,
			ServiceAccountUID:    pcr.Spec.ServiceAccountUID,
			NodeName:             pcr.Spec.NodeName,
			NodeUID:              pcr.Spec.NodeUID,
			MaxExpirationSeconds: pcr.Spec.MaxExpirationSeconds,
			StubPKCS10Request:    pcr.Spec.StubPKCS10Request,
		},
	}
	_, err := c.clientset.CertificatesV1beta1().PodCertificateRequests(c.namespace).Create(ctx, beta, metav1.CreateOptions{})
	return err
}

func (c *pcrClient) status(ctx context.Context, name string) (certsv1.PodCertificateRequestStatus, error) {
	if c.v1 {
		pcr, err := c.clientset.CertificatesV1().PodCertificateRequests(c.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return certsv1.PodCertificateRequestStatus{}, err
		}
		return pcr.Status, nil
	}
	pcr, err := c.clientset.CertificatesV1beta1().PodCertificateRequests(c.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return certsv1.PodCertificateRequestStatus{}, err
	}
	return certsv1.PodCertificateRequestStatus(pcr.Status), nil
}

func (c *pcrClient) delete(ctx context.Context, name string) error {
	if c.v1 {
		return c.clientset.CertificatesV1().PodCertificateRequests(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}
	return c.clientset.CertificatesV1beta1().PodCertificateRequests(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

// ensureRouterClientNamespace creates RouterClientNamespace, leaving it in
// place if it already exists. The namespace carries NamespaceLabel, so
// hack/cleanup-e2e.sh removes it with the rest.
func ensureRouterClientNamespace(ctx context.Context, clientset kubernetes.Interface) error {
	_, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   RouterClientNamespace,
			Labels: map[string]string{NamespaceLabel: "true"},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating namespace: %w", err)
	}
	return nil
}

// RouterServerName is the DNS name on the router's servicedns serving
// certificate.
func RouterServerName() string {
	return fmt.Sprintf("%s.%s.svc", ResourceName("atenet-router"), SystemNamespace())
}

// RouterTLSConfig returns a TLS config for the router's TLS listeners that
// verifies the router's serving certificate and presents clientCert, which
// may be nil to present none. The caller sets NextProtos.
func RouterTLSConfig(ctx context.Context, clientCert *tls.Certificate) (*tls.Config, error) {
	roots, err := ateclient.ServiceDNSTrustPool(ctx, GetClients().K8s)
	if err != nil {
		return nil, fmt.Errorf("loading the servicedns trust bundle: %w", err)
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: RouterServerName(),
	}
	if clientCert != nil {
		cfg.Certificates = []tls.Certificate{*clientCert}
	}
	return cfg, nil
}
