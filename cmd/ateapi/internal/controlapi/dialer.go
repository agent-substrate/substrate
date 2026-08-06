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

package controlapi

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"

	"github.com/agent-substrate/substrate/internal/atelet"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/lru"
)

var ErrWorkerPodNotFound = errors.New("worker pod not found")

// ErrNoAteletOnNode reports that the informer cache holds no atelet pod for
// the requested node — e.g. the atelet is restarting, or the node is gone.
// Distinct from ErrWorkerPodNotFound, which callers treat as crash-worthy;
// this one is retryable.
var ErrNoAteletOnNode = errors.New("no atelet pod found on node")

// The SPIFFE identity that atelet serving certs carry, as minted by the
// podidentity signer (cmd/podcertcontroller/internal/podidentitysigner).
// The namespace part is ateletNamespace, declared in informer.go.
const (
	trustDomainName = "cluster.local"
	ateletSA        = "atelet"
)

// AteletDialer handles gRPC connections to Atelet pods.
type AteletDialer struct {
	workerIndexer cache.Indexer
	ateletIndexer cache.Indexer
	ateletConns   *lru.Cache
	// dialCredentials builds the transport credentials used to dial a given
	// atelet, keyed on the atelet's expected pod UID. Production wires this to
	// per-atelet mTLS; tests can override it with insecure credentials.
	dialCredentials func(expectedPodUID string) (credentials.TransportCredentials, error)
}

// NewAteletDialer creates a new AteletDialer. clientBundlePath and serverCAPath
// are used to build the per-atelet mTLS credentials used for every atelet connection.
func NewAteletDialer(workerIndexer cache.Indexer, ateletIndexer cache.Indexer, clientBundlePath, serverCAPath string) *AteletDialer {
	return &AteletDialer{
		workerIndexer: workerIndexer,
		ateletIndexer: ateletIndexer,
		ateletConns:   lru.New(1024),
		dialCredentials: func(expectedPodUID string) (credentials.TransportCredentials, error) {
			tlsConfig, err := buildTLSConfig(clientBundlePath, serverCAPath, expectedPodUID)
			if err != nil {
				return nil, err
			}
			return credentials.NewTLS(tlsConfig), nil
		},
	}
}

// DialForWorker returns a gRPC connection to the Atelet running on the same node as the specified worker pod.
// Returns ErrWorkerPodNotFound if the worker pod is not found in the informer cache.
func (d *AteletDialer) DialForWorker(workerPodNamespace, workerPodName string) (*grpc.ClientConn, error) {
	workerPodKey := workerPodNamespace + "/" + workerPodName
	matchingPods, err := d.workerIndexer.ByIndex(byNamespaceAndName, workerPodKey)
	if err != nil {
		return nil, fmt.Errorf("while finding pod %q: %w", workerPodKey, err)
	}

	if len(matchingPods) == 0 {
		return nil, ErrWorkerPodNotFound
	}

	if len(matchingPods) > 1 {
		return nil, fmt.Errorf("expected 1 pod match, got %d", len(matchingPods))
	}

	selectedWorker := matchingPods[0].(*corev1.Pod)

	conn, err := d.DialForAteletOnNode(selectedWorker.Spec.NodeName)
	if err != nil {
		return nil, fmt.Errorf("for worker pod %q: %w", workerPodKey, err)
	}
	return conn, nil
}

// DialForAteletOnNode resolves the single atelet pod on nodeName and dials it
// with per-atelet pod-UID-pinned credentials, caching the connection by the
// atelet's pod UID. Used directly when an actor has no worker assignment but
// its state is pinned to a node — e.g. a PAUSED actor whose local snapshot
// lives there. Returns ErrNoAteletOnNode if the informer cache holds no
// atelet pod for the node.
func (d *AteletDialer) DialForAteletOnNode(nodeName string) (*grpc.ClientConn, error) {
	matchingAtelets, err := d.ateletIndexer.ByIndex(byNode, nodeName)
	if err != nil {
		return nil, fmt.Errorf("while finding atelet on node %q: %w", nodeName, err)
	}

	if len(matchingAtelets) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNoAteletOnNode, nodeName)
	}
	if len(matchingAtelets) > 1 {
		return nil, fmt.Errorf("found %d atelet pods on node %q, expected 1", len(matchingAtelets), nodeName)
	}

	selectedAtelet := matchingAtelets[0].(*corev1.Pod)
	ateletKey := string(selectedAtelet.ObjectMeta.UID)

	ateletConnAny, ok := d.ateletConns.Get(ateletKey)
	if ok {
		return ateletConnAny.(*grpc.ClientConn), nil
	}

	if len(selectedAtelet.Status.PodIPs) == 0 {
		return nil, fmt.Errorf("selected atelet %q has no assigned IPs", selectedAtelet.ObjectMeta.Namespace+"/"+selectedAtelet.ObjectMeta.Name)
	}

	creds, err := d.dialCredentials(string(selectedAtelet.ObjectMeta.UID))
	if err != nil {
		return nil, fmt.Errorf("while building atelet credentials: %w", err)
	}

	ateletConn, err := grpc.NewClient(
		net.JoinHostPort(selectedAtelet.Status.PodIPs[0].IP, strconv.Itoa(atelet.DefaultPort)),
		grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet gRPC client connection: %w", err)
	}

	d.ateletConns.Add(ateletKey, ateletConn)

	return ateletConn, nil
}

func buildTLSConfig(clientBundlePath, serverCAPath, expectedPodUID string) (*tls.Config, error) {
	trustDomain, err := spiffeid.TrustDomainFromString(trustDomainName)
	if err != nil {
		return nil, fmt.Errorf("while parsing trust domain %q: %w", trustDomainName, err)
	}
	bundle, err := x509bundle.Load(trustDomain, serverCAPath)
	if err != nil {
		return nil, fmt.Errorf("while loading CA bundle from %s: %w", serverCAPath, err)
	}
	expectedID, err := spiffeid.FromSegments(trustDomain, "ns", ateletNamespace, "sa", ateletSA)
	if err != nil {
		return nil, fmt.Errorf("while building expected atelet SPIFFE ID: %w", err)
	}

	verify, err := verifyAteletServerCert(bundle, expectedID, expectedPodUID)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet server cert verifier: %w", err)
	}

	tlsConfig := tls.Config{
		MinVersion:           tls.VersionTLS13,
		GetClientCertificate: credbundle.ClientLoader(clientBundlePath),
		// Skip the default verification because the peer is dialed by IP and its
		// certificate has no DNS/IP SAN.
		InsecureSkipVerify: true,
		VerifyConnection:   verify,
	}

	return &tlsConfig, nil
}

func verifyAteletServerCert(bundle *x509bundle.Bundle, expectedID spiffeid.ID, expectedPodUID string) (func(tls.ConnectionState) error, error) {
	if expectedPodUID == "" {
		return nil, fmt.Errorf("expected pod UID must not be empty")
	}
	if expectedID.IsZero() {
		return nil, fmt.Errorf("expected pod spiffe ID must not be empty")
	}
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("server presented no certificate")
		}
		id, _, err := x509svid.Verify(cs.PeerCertificates, bundle)
		if err != nil {
			return fmt.Errorf("verifying server certificate chain: %w", err)
		}
		if id != expectedID {
			return fmt.Errorf("server SPIFFE ID %q does not match expected %q", id, expectedID)
		}

		leaf := cs.PeerCertificates[0]
		if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
			return fmt.Errorf("server certificate lacks the serverAuth extended key usage")
		}

		identity, err := substratex509.PodIdentityFromCertificate(leaf)
		if err != nil {
			return fmt.Errorf("failed to parse PodIdentity extension: %w", err)
		}
		if identity == nil {
			return fmt.Errorf("server certificate has no PodIdentity extension, expected pod UID %q", expectedPodUID)
		}
		if identity.PodUID != expectedPodUID {
			return fmt.Errorf("pod UID %q does not match expected %q", identity.PodUID, expectedPodUID)
		}

		return nil
	}, nil
}
