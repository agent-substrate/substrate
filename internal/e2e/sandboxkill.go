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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// testserverImage resolves the e2e testserver image, whose kill subcommand
// does the killing.
func testserverImage(t *testing.T) string {
	t.Helper()
	manifest := filepath.Join(t.TempDir(), "image.yaml")
	if err := os.WriteFile(manifest, []byte("image: ko://go.ate.dev/substrate/internal/e2e/fixtures/testserver\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// the first line; ko appends a document separator
	line, _, _ := strings.Cut(string(koResolve(t, manifest)), "\n")
	image := strings.TrimSpace(strings.TrimPrefix(line, "image:"))
	if image == "" {
		t.Fatal("ko resolved no testserver image")
	}
	return image
}

// KillActorSandbox kills the actor's sandbox processes on its worker, the way a
// crash would, from an ephemeral container sharing the worker's PID namespace.
func KillActorSandbox(ctx context.Context, t *testing.T, clients *Clients, namespace, podName, actorUID string) {
	t.Helper()
	pods := clients.K8s.CoreV1().Pods(namespace)
	pod, err := pods.Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting worker pod %s/%s: %v", namespace, podName, err)
	}
	name := fmt.Sprintf("kill-sandbox-%d", len(pod.Spec.EphemeralContainers))
	pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, corev1.EphemeralContainer{
		TargetContainerName: "worker",
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name:  name,
			Image: testserverImage(t),
			// kills the runsc sandbox and gofer, or cloud-hypervisor and
			// virtiofsd: each names the actor UID in its command line
			Args: []string{"kill", "--cmdline-contains", actorUID},
			// root, like the sandbox processes, which also run unconfined, and
			// the default AppArmor profile, enforced on GKE, may not signal them
			SecurityContext: &corev1.SecurityContext{
				RunAsUser:       ptr.To[int64](0),
				AppArmorProfile: &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined},
			},
		},
	})
	if _, err := pods.UpdateEphemeralContainers(ctx, podName, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("adding ephemeral container to %s/%s: %v", namespace, podName, err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		pod, err := pods.Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting worker pod %s/%s: %v", namespace, podName, err)
		}
		for _, st := range pod.Status.EphemeralContainerStatuses {
			if st.Name != name {
				continue
			}
			if w := st.State.Waiting; w != nil && w.Reason != "" && w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" {
				t.Fatalf("kill container for actor %s cannot start: %s: %s", actorUID, w.Reason, w.Message)
			}
			if st.State.Terminated == nil {
				continue
			}
			if code := st.State.Terminated.ExitCode; code != 0 {
				t.Fatalf("killing the sandbox of actor %s exited %d", actorUID, code)
			}
			t.Logf("Killed the sandbox of actor %s on %s/%s", actorUID, namespace, podName)
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out killing the sandbox of actor %s", actorUID)
}
