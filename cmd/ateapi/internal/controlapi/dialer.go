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
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/agent-substrate/substrate/internal/atelet"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/lru"
)

// ErrNoAteletOnNode reports that the informer cache holds no atelet pod for
// the requested node — e.g. the atelet is restarting, or the node is gone.
// Retryable.
var ErrNoAteletOnNode = errors.New("no atelet pod found on node")

// ateletConn is a cached connection and the pod IP it was dialed on.
type ateletConn struct {
	ip   string
	conn *grpc.ClientConn
}

// AteletDialer handles gRPC connections to Atelet pods.
type AteletDialer struct {
	ateletIndexer cache.Indexer
	// mu makes the lookup, stale eviction and insert in DialForAteletOnNode
	// atomic, so concurrent callers cannot evict each other's fresh conn.
	mu sync.Mutex
	// ateletConns holds one *ateletConn per atelet pod UID.
	ateletConns *lru.Cache
	// dialCredentials builds the transport credentials used to dial a given
	// atelet, keyed on the atelet's expected pod UID. Production wires this to
	// per-atelet mTLS; tests can override it with insecure credentials.
	dialCredentials func(expectedPodUID string) (credentials.TransportCredentials, error)
}

// DialerOption customizes an AteletDialer built by NewAteletDialer.
type DialerOption func(*AteletDialer)

// WithDialCredentials overrides how transport credentials are built for a given
// atelet pod UID. Tests use it to reach a fake atelet over insecure transport
// while still exercising the real lookup, dial and connection-cache path.
func WithDialCredentials(build func(expectedPodUID string) (credentials.TransportCredentials, error)) DialerOption {
	return func(d *AteletDialer) { d.dialCredentials = build }
}

// NewAteletDialer creates a new AteletDialer. clientBundlePath and serverCAPath
// are used to build the per-atelet mTLS credentials used for every atelet
// connection, and ateletSPIFFEID is the identity those credentials expect on
// the atelet serving cert.
func NewAteletDialer(ateletIndexer cache.Indexer, ateletSPIFFEID, clientBundlePath, serverCAPath string, opts ...DialerOption) *AteletDialer {
	d := &AteletDialer{
		ateletIndexer: ateletIndexer,
		ateletConns:   newAteletConnCache(1024),
		dialCredentials: func(expectedPodUID string) (credentials.TransportCredentials, error) {
			tlsConfig, err := buildTLSConfig(ateletSPIFFEID, clientBundlePath, serverCAPath, expectedPodUID)
			if err != nil {
				return nil, err
			}
			return credentials.NewTLS(tlsConfig), nil
		},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// newAteletConnCache builds the atelet conn cache, which closes the
// connections it evicts. A conn pushed out of the LRU without Close is not
// reclaimed: grpc keeps most of its goroutines and buffers alive for the
// life of the process. Closing on eviction can fail an RPC still in flight
// on a conn that aged to the LRU tail, but that failure is visible and
// retryable, unlike the leak.
//
// TODO: Consider pool semantics instead of a cache: a conn evicted for
// capacity would drain — close only once its last in-flight RPC finishes
// (e.g. refcounted checkout/release) — rather than being closed out from
// under a caller. Worth revisiting if the retryable eviction failures show
// up in practice.
func newAteletConnCache(size int) *lru.Cache {
	return lru.NewWithEvictionFunc(size, func(_ lru.Key, value interface{}) {
		_ = value.(*ateletConn).conn.Close()
	})
}

// DialForAteletOnNode resolves the single atelet pod on nodeName and dials it
// with per-atelet pod-UID-pinned credentials, caching the connection by the
// atelet's pod UID. A pod that keeps its UID but gets a new IP, such as after a
// node restart, is redialed.
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
	if len(selectedAtelet.Status.PodIPs) == 0 {
		return nil, fmt.Errorf("selected atelet %q has no assigned IPs", selectedAtelet.ObjectMeta.Namespace+"/"+selectedAtelet.ObjectMeta.Name)
	}
	ateletKey := string(selectedAtelet.ObjectMeta.UID)
	ateletIP := selectedAtelet.Status.PodIPs[0].IP

	d.mu.Lock()
	defer d.mu.Unlock()

	if cached, ok := d.ateletConns.Get(ateletKey); ok {
		if c := cached.(*ateletConn); c.ip == ateletIP {
			return c.conn, nil
		}
		// Remove closes the stale connection.
		d.ateletConns.Remove(ateletKey)
	}

	creds, err := d.dialCredentials(string(selectedAtelet.ObjectMeta.UID))
	if err != nil {
		return nil, fmt.Errorf("while building atelet credentials: %w", err)
	}

	conn, err := grpc.NewClient(
		net.JoinHostPort(ateletIP, strconv.Itoa(atelet.DefaultPort)),
		grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet gRPC client connection: %w", err)
	}

	d.ateletConns.Add(ateletKey, &ateletConn{ip: ateletIP, conn: conn})

	return conn, nil
}

func buildTLSConfig(ateletSPIFFEID, clientBundlePath, serverCAPath, expectedPodUID string) (*tls.Config, error) {
	verify, err := verifyAteletServerCert(ateletSPIFFEID, expectedPodUID)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet server cert verifier: %w", err)
	}
	return credbundle.PrepareClientTLSConfig(credbundle.ClientConfig{
		GetClientCertificate: credbundle.ClientLoader(clientBundlePath),
		TrustBundlePath:      serverCAPath,
		VerifyPeer:           verify,
	})
}

// verifyAteletServerCert checks the identities that chain verification cannot
// express: atelet is dialed by IP, so its certificate has no DNS/IP SAN, and
// a pod UID is not something an X.509 chain check can see at all.
func verifyAteletServerCert(expectedSPIFFEID, expectedPodUID string) (func(tls.ConnectionState) error, error) {
	if expectedPodUID == "" {
		return nil, fmt.Errorf("expected pod UID must not be empty")
	}
	if expectedSPIFFEID == "" {
		return nil, fmt.Errorf("expected pod spiffe ID must not be empty")
	}
	return func(cs tls.ConnectionState) error {
		leaf := cs.PeerCertificates[0]
		if len(leaf.URIs) != 1 || leaf.URIs[0].String() != expectedSPIFFEID {
			return fmt.Errorf("server SPIFFE ID does not match expected %q", expectedSPIFFEID)
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
