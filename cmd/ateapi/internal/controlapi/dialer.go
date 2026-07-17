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
	"encoding/asn1"
	"errors"
	"fmt"

	"os"

	"github.com/agent-substrate/substrate/internal/credbundle"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/lru"
)

var ErrWorkerPodNotFound = errors.New("worker pod not found")

// oidPodUID represents the Pod UID within a certificate extension.
var oidPodUID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 11129, 2, 6, 1, 3}

type customExtensionVal struct {
	S string `asn1:"utf8"`
}

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

	matchingAtelets, err := d.ateletIndexer.ByIndex(byNode, selectedWorker.Spec.NodeName)
	if err != nil {
		return nil, fmt.Errorf("while finding atelet for worker pod %q on node %q: %w", workerPodKey, selectedWorker.Spec.NodeName, err)
	}

	if len(matchingAtelets) != 1 {
		return nil, fmt.Errorf("found %d atelet pods on node %q, expected 1", len(matchingAtelets), selectedWorker.Spec.NodeName)
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
		selectedAtelet.Status.PodIPs[0].IP+":8085",
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
	roots, err := caPoolFromFile(serverCAPath)
	if err != nil {
		return nil, err
	}

	tlsConfig := tls.Config{
		MinVersion:           tls.VersionTLS13,
		GetClientCertificate: credbundle.ClientLoader(clientBundlePath),
		// Skip the default verification because the peer is dialed by IP and its
		// certificate has no DNS/IP SAN.
		InsecureSkipVerify: true,
		RootCAs:            roots,
		VerifyConnection:   verifyAteletServerCert(roots, expectedPodUID),
	}

	return &tlsConfig, nil
}

func verifyAteletServerCert(roots *x509.CertPool, expectedPodUID string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("server presented no certificate")
		}
		leaf := cs.PeerCertificates[0]
		intermediates := x509.NewCertPool()
		for _, c := range cs.PeerCertificates[1:] {
			intermediates.AddCert(c)
		}
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); err != nil {
			return fmt.Errorf("verifying server certificate chain: %w", err)
		}

		var foundPodUID customExtensionVal
		for _, ext := range leaf.Extensions {
			if ext.Id.Equal(oidPodUID) {
				if _, err := asn1.Unmarshal(ext.Value, &foundPodUID); err != nil {
					return fmt.Errorf("failed to parse oidPodUID extension: %w", err)
				}
			}
		}
		if foundPodUID.S == "" || expectedPodUID == "" {
			return fmt.Errorf("found Pod UID == %q, expected podUID == %q", foundPodUID.S, expectedPodUID)
		}
		if foundPodUID.S != expectedPodUID {
			return fmt.Errorf("pod UID is %q does not match expected %q", foundPodUID.S, expectedPodUID)
		}

		return nil
	}
}

func caPoolFromFile(path string) (*x509.CertPool, error) {
	caBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA bundle from %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("failed to parse CA bundle from %s", path)
	}
	return pool, nil
}
