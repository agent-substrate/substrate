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

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/portforward"
)

// WaitForPodReady blocks until the pod passes its readiness probe, and fails the
// test with the pod's last observed state if it does not within timeout.
//
// A suite that skipped this and dialed straight away would race the readiness
// probe, and a fixture that is still pulling its image or crash-looping would
// then be reported as whatever the code under test does with a refused
// connection. Reporting the container's own waiting/terminated reason instead is
// the whole point of the poll: it is the difference between "ImagePullBackOff"
// and an unexplained timeout.
func WaitForPodReady(t *testing.T, ctx context.Context, namespace, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		pod, err := GetClients().K8s.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		switch {
		case err != nil:
			lastState = err.Error()
		case portforward.IsPodReady(pod):
			t.Logf("pod %s/%s is ready", namespace, name)
			return
		default:
			lastState = DescribePodState(pod)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out after %v waiting for pod %s/%s to become ready: %s", timeout, namespace, name, lastState)
}

// DescribePodState summarizes why a pod is not ready yet, one clause per
// container, for a timeout message.
func DescribePodState(pod *corev1.Pod) string {
	parts := []string{"phase=" + string(pod.Status.Phase)}
	for _, cs := range pod.Status.ContainerStatuses {
		switch {
		case cs.State.Waiting != nil:
			parts = append(parts, fmt.Sprintf("%s waiting: %s: %s", cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message))
		case cs.State.Terminated != nil:
			parts = append(parts, fmt.Sprintf("%s terminated: %s: %s", cs.Name, cs.State.Terminated.Reason, cs.State.Terminated.Message))
		default:
			parts = append(parts, fmt.Sprintf("%s running, ready=%t", cs.Name, cs.Ready))
		}
	}
	return strings.Join(parts, "; ")
}
