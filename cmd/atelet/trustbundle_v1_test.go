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

package main

import (
	"testing"

	certsv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestV1InformerProjectsAndQueuesTrustBundle(t *testing.T) {
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &certsv1.ClusterTrustBundle{}, 0, cache.Indexers{})
	raw := string(testCertPEM(t))
	ctb := &certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
		Spec:       certsv1.ClusterTrustBundleSpec{TrustBundle: raw},
	}
	if err := informer.GetIndexer().Add(ctb); err != nil {
		t.Fatal(err)
	}
	r := newSystemInfoVolumeRefresher(ctbInformerGetter{informer, true}, nil)
	defer r.queue.ShutDown()
	_, got, err := rawTrustBundle(r.lister, EgressTrustBundleName)
	if err != nil || got != raw {
		t.Fatalf("v1 raw trust bundle = %q, %v", got, err)
	}
	r.eventHandler().OnAdd(ctb, false)
	if r.queue.Len() != 1 {
		t.Fatalf("v1 event queued %d bundles, want 1", r.queue.Len())
	}
}
