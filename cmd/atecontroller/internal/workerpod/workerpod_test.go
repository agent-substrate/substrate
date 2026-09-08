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

package workerpod

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/ateattr"
	corev1 "k8s.io/api/core/v1"
)

// The ateom status is what every sandbox-replacement check reads, so picking
// the wrong container — or an absent one — silently decides that nothing
// happened.
func TestAteomStatus(t *testing.T) {
	tests := []struct {
		name     string
		statuses []corev1.ContainerStatus
		wantID   string
		wantNil  bool
	}{
		{
			name:    "no statuses reported yet",
			wantNil: true,
		},
		{
			name:     "another container only",
			statuses: []corev1.ContainerStatus{{Name: "sidecar", ContainerID: "containerd://other"}},
			wantNil:  true,
		},
		{
			name:     "found by name",
			statuses: []corev1.ContainerStatus{{Name: AteomContainerName, ContainerID: "containerd://aaaa"}},
			wantID:   "containerd://aaaa",
		},
		{
			name: "not by position",
			statuses: []corev1.ContainerStatus{
				{Name: "sidecar", ContainerID: "containerd://other"},
				{Name: AteomContainerName, ContainerID: "containerd://aaaa"},
			},
			wantID: "containerd://aaaa",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: tt.statuses}}
			got := AteomStatus(pod)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("AteomStatus() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("AteomStatus() = nil, want the ateom status")
			}
			if got.ContainerID != tt.wantID {
				t.Errorf("ContainerID = %q, want %q", got.ContainerID, tt.wantID)
			}
		})
	}
}

// The caller has already established from the container id that the sandbox was
// replaced, so this never reports empty: an empty reason would read downstream
// as no termination at all.
func TestSandboxTerminatedReason(t *testing.T) {
	tests := []struct {
		name   string
		status *corev1.ContainerStatus
		want   string
	}{
		{
			name: "kubelet reason",
			status: &corev1.ContainerStatus{
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"},
				},
			},
			want: "OOMKilled",
		},
		{
			name: "terminated with no reason",
			status: &corev1.ContainerStatus{
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{},
				},
			},
			want: ateattr.ContainerTerminationReasonOther,
		},
		{
			name: "reason from a newer kubelet is bounded",
			status: &corev1.ContainerStatus{
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "SomethingNew"},
				},
			},
			want: ateattr.ContainerTerminationReasonOther,
		},
		{
			name:   "never terminated",
			status: &corev1.ContainerStatus{},
			want:   ateattr.ContainerTerminationReasonOther,
		},
		{
			name:   "no status at all",
			status: nil,
			want:   ateattr.ContainerTerminationReasonOther,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SandboxTerminatedReason(tt.status); got != tt.want {
				t.Errorf("SandboxTerminatedReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Substrate defers to the kubelet's own verdict rather than counting restarts,
// so this must read exactly that and nothing adjacent: a container that is
// merely restarting, or waiting for any other reason, is not a crash loop.
func TestInCrashLoop(t *testing.T) {
	waiting := func(reason string) *corev1.ContainerStatus {
		return &corev1.ContainerStatus{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
		}
	}
	tests := []struct {
		name   string
		status *corev1.ContainerStatus
		want   bool
	}{
		{name: "kubelet gave up", status: waiting(CrashLoopBackOff), want: true},
		{name: "still pulling", status: waiting("ImagePullBackOff"), want: false},
		{name: "starting", status: waiting("ContainerCreating"), want: false},
		{name: "no reason", status: waiting(""), want: false},
		{name: "running", status: &corev1.ContainerStatus{
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}, want: false},
		{name: "restarted but running again", status: &corev1.ContainerStatus{
			RestartCount: 7,
			State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}, want: false},
		{name: "no status", status: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InCrashLoop(tt.status); got != tt.want {
				t.Errorf("InCrashLoop() = %v, want %v", got, tt.want)
			}
		})
	}
}
