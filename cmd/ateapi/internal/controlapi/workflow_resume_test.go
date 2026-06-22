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
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func pool(namespace, name string, labels map[string]string) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    labels,
		},
	}
}

const (
	testDigestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testDigestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestActorTemplateImageDigests(t *testing.T) {
	actorTemplate := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{
			PauseImage: "example.com/pause@" + testDigestA,
			Containers: []atev1alpha1.Container{
				{Image: "example.com/app@" + testDigestB},
				{Image: "example.com/duplicate@" + testDigestA},
			},
		},
	}
	got, err := actorTemplateImageDigests(actorTemplate)
	if err != nil {
		t.Fatalf("actorTemplateImageDigests failed: %v", err)
	}
	if len(got) != 2 || got[0] != testDigestA || got[1] != testDigestB {
		t.Fatalf("actorTemplateImageDigests = %v, want [%s %s]", got, testDigestA, testDigestB)
	}
}

func TestFindFreeWorkerPrefersCompleteImageCacheHit(t *testing.T) {
	step := &AssignWorkerStep{}
	eligible := map[types.NamespacedName]struct{}{{Namespace: "ns", Name: "pool"}: {}}
	workers := []*ateapipb.Worker{
		{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "cold", NodeName: "node-cold"},
		{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "warm", NodeName: "node-warm"},
	}
	caches := map[string]*ateapipb.NodeImageCache{
		"node-warm": {NodeName: "node-warm", ImageDigests: []string{testDigestA, testDigestB}},
	}

	got := step.findFreeWorker(workers, eligible, nil, []string{testDigestA, testDigestB}, caches)
	if got.GetWorkerPod() != "warm" {
		t.Fatalf("findFreeWorker selected %q, want warm", got.GetWorkerPod())
	}
}

func TestFindFreeWorkerSnapshotRestrictionPrecedesImageAffinity(t *testing.T) {
	step := &AssignWorkerStep{}
	eligible := map[types.NamespacedName]struct{}{{Namespace: "ns", Name: "pool"}: {}}
	workers := []*ateapipb.Worker{
		{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "snapshot-local", NodeName: "node-snapshot"},
		{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "warm", NodeName: "node-warm"},
	}
	caches := map[string]*ateapipb.NodeImageCache{
		"node-warm": {NodeName: "node-warm", ImageDigests: []string{testDigestA}},
	}

	got := step.findFreeWorker(workers, eligible, []string{"node-snapshot"}, []string{testDigestA}, caches)
	if got.GetWorkerPod() != "snapshot-local" {
		t.Fatalf("findFreeWorker selected %q, want snapshot-local", got.GetWorkerPod())
	}
}

func TestHasAllImageDigestsRejectsPartialHit(t *testing.T) {
	cache := &ateapipb.NodeImageCache{ImageDigests: []string{testDigestA}}
	if hasAllImageDigests(cache, []string{testDigestA, testDigestB}) {
		t.Fatal("partial image-cache hit was treated as a complete hit")
	}
}

func TestLoadNodeImageCachesDeduplicatesNodes(t *testing.T) {
	store, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	if err := store.SetNodeImageCache(context.Background(), &ateapipb.NodeImageCache{
		NodeName: "node-1", ImageDigests: []string{testDigestA},
	}, time.Minute); err != nil {
		t.Fatalf("SetNodeImageCache failed: %v", err)
	}
	step := &AssignWorkerStep{store: store}
	got := step.loadNodeImageCaches(context.Background(), []*ateapipb.Worker{
		{NodeName: "node-1"}, {NodeName: "node-1"}, {NodeName: "node-missing"},
	})
	if len(got) != 1 || got["node-1"] == nil {
		t.Fatalf("loadNodeImageCaches = %v, want only node-1", got)
	}
}

func TestEligibleWorkerPools(t *testing.T) {
	tests := []struct {
		name              string
		pools             []*atev1alpha1.WorkerPool
		templateSelector  *metav1.LabelSelector
		actorSelector     *ateapipb.Selector
		wantEligibleNames []string // pool names expected in the result
	}{
		{
			name: "both nil matches everything",
			pools: []*atev1alpha1.WorkerPool{
				pool("ns", "a", map[string]string{"foo": "bar"}),
				pool("ns", "b", nil),
			},
			templateSelector:  nil,
			actorSelector:     nil,
			wantEligibleNames: []string{"a", "b"},
		},
		{
			name: "template selector only",
			pools: []*atev1alpha1.WorkerPool{
				pool("ns", "match", map[string]string{"workload": "code-sandbox"}),
				pool("ns", "nomatch", map[string]string{"workload": "browser-agent"}),
			},
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector:     nil,
			wantEligibleNames: []string{"match"},
		},
		{
			name: "actor selector only",
			pools: []*atev1alpha1.WorkerPool{
				pool("ns", "match", map[string]string{"tier": "paid"}),
				pool("ns", "nomatch", map[string]string{"tier": "free"}),
			},
			templateSelector: nil,
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligibleNames: []string{"match"},
		},
		{
			name: "AND of two selectors on the same pool",
			pools: []*atev1alpha1.WorkerPool{
				pool("ns", "both", map[string]string{"workload": "code-sandbox", "tier": "paid"}),
				pool("ns", "template-only", map[string]string{"workload": "code-sandbox", "tier": "free"}),
				pool("ns", "actor-only", map[string]string{"workload": "browser-agent", "tier": "paid"}),
				pool("ns", "neither", map[string]string{"workload": "browser-agent", "tier": "free"}),
			},
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligibleNames: []string{"both"},
		},
		{
			name: "disjoint label keys: independent evaluation, not a merged map",
			pools: []*atev1alpha1.WorkerPool{
				// Has the template's key/value but not the actor's.
				pool("ns", "template-key-only", map[string]string{"workload": "x"}),
				// Has the actor's key/value but not the template's.
				pool("ns", "actor-key-only", map[string]string{"zone": "y"}),
				// Has both keys with matching values: must be the only eligible pool.
				pool("ns", "both-keys", map[string]string{"workload": "x", "zone": "y"}),
			},
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "x"},
			},
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"zone": "y"},
			},
			wantEligibleNames: []string{"both-keys"},
		},
		{
			name: "no eligible pool",
			pools: []*atev1alpha1.WorkerPool{
				pool("ns", "a", map[string]string{"workload": "x"}),
			},
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "y"},
			},
			actorSelector:     nil,
			wantEligibleNames: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eligibleWorkerPools(tt.pools, tt.templateSelector, tt.actorSelector)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			wantKeys := make(map[types.NamespacedName]struct{}, len(tt.wantEligibleNames))
			for _, name := range tt.wantEligibleNames {
				wantKeys[types.NamespacedName{Namespace: "ns", Name: name}] = struct{}{}
			}

			if len(got) != len(wantKeys) {
				t.Fatalf("got %d eligible pools, want %d: got=%v want=%v", len(got), len(wantKeys), got, wantKeys)
			}
			for k := range wantKeys {
				if _, ok := got[k]; !ok {
					t.Errorf("expected pool %v to be eligible, but it was not: got=%v", k, got)
				}
			}
		})
	}
}
