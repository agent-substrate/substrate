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

package kube

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// The delete is foreground, so the Deployment outlives its pods and its
// absence means they are gone too: the wait lasts until it is.
func TestDeleteDeploymentAndWait(t *testing.T) {
	shortPollInterval(t)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "fake-workersync"}}
	cs := kubefake.NewClientset(dep)
	// The Deployment lingers, as while the garbage collector deletes its
	// pods, for the first few reads after the delete.
	lingering, gets := 3, 0
	cs.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets > lingering {
			return false, nil, nil
		}
		terminating := dep.DeepCopy()
		now := metav1.Now()
		terminating.DeletionTimestamp = &now
		return true, terminating, nil
	})
	c := &Client{Typed: cs}

	if err := c.DeleteDeploymentAndWait(context.Background(), "ate-system", "fake-workersync", time.Second); err != nil {
		t.Fatalf("DeleteDeploymentAndWait() error = %v", err)
	}
	if gets <= lingering {
		t.Errorf("DeleteDeploymentAndWait read the Deployment %d times, returning while it still existed", gets)
	}
	if dep, err := c.GetDeployment(context.Background(), "ate-system", "fake-workersync"); err != nil || dep != nil {
		t.Errorf("after DeleteDeploymentAndWait, GetDeployment() = %v, %v, want nil, nil", dep, err)
	}
	var policy *metav1.DeletionPropagation
	for _, action := range cs.Actions() {
		if del, ok := action.(k8stesting.DeleteAction); ok {
			policy = del.GetDeleteOptions().PropagationPolicy
		}
	}
	if policy == nil || *policy != metav1.DeletePropagationForeground {
		t.Errorf("delete propagation = %v, want %s", policy, metav1.DeletePropagationForeground)
	}

	if err := c.DeleteDeploymentAndWait(context.Background(), "ate-system", "fake-workersync", time.Second); err != nil {
		t.Errorf("DeleteDeploymentAndWait() of a missing Deployment = %v, want nil", err)
	}
}
