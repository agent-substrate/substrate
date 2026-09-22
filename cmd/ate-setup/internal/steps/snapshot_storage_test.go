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

package steps

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

func TestWaitSnapshotStorage(t *testing.T) {
	for _, tc := range []struct {
		name        string
		kind        bool
		ready       bool
		condition   batchv1.JobCondition
		wantError   string
		wantTimeout bool
	}{
		{name: "non-kind skips local storage"},
		{name: "completed initialization", kind: true, ready: true, condition: batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
		{name: "storage not ready", kind: true, wantError: "deployment/rustfs", wantTimeout: true},
		{name: "initialization still running", kind: true, ready: true, wantError: "job/rustfs-bucket-init", wantTimeout: true},
		{name: "completion condition false", kind: true, ready: true, condition: batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionFalse}, wantError: "job/rustfs-bucket-init", wantTimeout: true},
		{name: "initialization failed", kind: true, ready: true, condition: batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "bucket creation failed"}, wantError: "BackoffLimitExceeded: bucket creation failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "rustfs", Namespace: NamespaceAteSystem},
			}
			if tc.ready {
				deployment.Status = appsv1.DeploymentStatus{Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1}
			}
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "rustfs-bucket-init", Namespace: NamespaceAteSystem},
				Status:     batchv1.JobStatus{Conditions: []batchv1.JobCondition{tc.condition}},
			}
			client := fake.NewClientset(deployment, job)
			e := &Env{
				Cfg:  &config.Config{Kind: tc.kind, RolloutTimeout: 20 * time.Millisecond},
				Kube: &kube.Client{Typed: client},
			}
			err := e.waitSnapshotStorage(context.Background())
			if tc.wantError == "" {
				if err != nil {
					t.Fatalf("waitSnapshotStorage: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("waitSnapshotStorage = %v, want error containing %q", err, tc.wantError)
			}
			if tc.wantTimeout && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("error = %v, want deadline exceeded", err)
			}
			if !tc.kind && len(client.Actions()) != 0 {
				t.Errorf("non-KIND install accessed local storage: %v", client.Actions())
			}
		})
	}
}
