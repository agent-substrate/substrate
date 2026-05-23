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
	"errors"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/lru"
)

var ErrWorkerPodNotFound = errors.New("worker pod not found")

// AteletDialer handles gRPC connections to Atelet pods.
type AteletDialer struct {
	workerIndexer cache.Indexer
	ateletIndexer cache.Indexer
	ateletConns   *lru.Cache
}

// NewAteletDialer creates a new AteletDialer.
func NewAteletDialer(workerIndexer cache.Indexer, ateletIndexer cache.Indexer) *AteletDialer {
	return &AteletDialer{
		workerIndexer: workerIndexer,
		ateletIndexer: ateletIndexer,
		ateletConns:   lru.New(1024),
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
	ateletKey := selectedAtelet.ObjectMeta.Namespace + "/" + selectedAtelet.ObjectMeta.Name

	ateletConnAny, ok := d.ateletConns.Get(ateletKey)
	if ok {
		return ateletConnAny.(*grpc.ClientConn), nil
	}

	if len(selectedAtelet.Status.PodIPs) == 0 {
		return nil, fmt.Errorf("selected atelet %q has no assigned IPs: %w", selectedAtelet.ObjectMeta.Namespace+"/"+selectedAtelet.ObjectMeta.Name, err)
	}

	ateletConn, err := grpc.NewClient(
		selectedAtelet.Status.PodIPs[0].IP+":8085",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet gRPC client connection: %w", err)
	}

	d.ateletConns.Add(ateletKey, ateletConn)

	return ateletConn, nil
}
