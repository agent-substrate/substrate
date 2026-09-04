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

// Package workerpod reads the parts of a worker Pod that both the WorkerPool
// controller writing it and the syncer watching it have to agree on.
package workerpod

import (
	"github.com/agent-substrate/substrate/internal/ateattr"
	corev1 "k8s.io/api/core/v1"
)

// AteomContainerName is the sole container of a worker pod. The controller
// names it and the syncer looks its status up by it, so it is shared rather
// than spelled twice: a rename that only landed on one side would make the
// syncer's sandbox check a permanent no-op with nothing failing.
const AteomContainerName = "ateom"

// AteomStatus returns the kubelet's status for the ateom container, or nil
// before the kubelet has reported one. By name rather than by index: the pod
// has one container today, and an index would silently read the wrong one on
// the day it does not.
func AteomStatus(pod *corev1.Pod) *corev1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == AteomContainerName {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// SandboxTerminatedReason is the kubelet's word for how the previous ateom
// container ended, normalized onto the bounded set because it reaches
// ate.actor.crashes as a label. A container that never terminated, or one the
// kubelet gave no reason for, reports _OTHER rather than empty: the caller has
// already established from the container id that the sandbox was replaced, and
// an empty reason would report that as no termination at all.
func SandboxTerminatedReason(status *corev1.ContainerStatus) string {
	var reason string
	if status != nil && status.LastTerminationState.Terminated != nil {
		reason = status.LastTerminationState.Terminated.Reason
	}
	return ateattr.NormalizeContainerTerminationReason(reason)
}

// CrashLoopBackOff is the kubelet's own verdict that a container is failing
// faster than it is worth restarting. Substrate defers to it rather than
// counting restarts itself: the kubelet already backs off exponentially and
// resets once a container stays up, and a second threshold here would only
// disagree with it.
const CrashLoopBackOff = "CrashLoopBackOff"

// InCrashLoop reports whether the kubelet has given up restarting the ateom for
// now. A Worker in this state must not be handed more Actors: its sandbox will
// keep disappearing, and every Actor placed on it is destroyed for good, since
// ACTOR_STATE_CRASHED is terminal.
func InCrashLoop(status *corev1.ContainerStatus) bool {
	if status == nil || status.State.Waiting == nil {
		return false
	}
	return status.State.Waiting.Reason == CrashLoopBackOff
}
