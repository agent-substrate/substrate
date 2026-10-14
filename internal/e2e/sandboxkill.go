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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// killSandboxScript SIGKILLs every process whose command line names the actor:
// the runsc sandbox and gofer, or cloud-hypervisor and virtiofsd. The UID comes
// from the environment so the script never matches itself.
const killSandboxScript = `n=0
for d in /proc/[0-9]*; do
  c=$(tr '\0' ' ' < "$d/cmdline" 2>/dev/null)
  case "$c" in *"$ACTOR_UID"*) kill -9 "${d#/proc/}" && n=$((n+1)) ;; esac
done
echo "killed $n"
[ "$n" -gt 0 ]`

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
			Name:    name,
			Image:   "busybox@sha256:1487d0af5f52b4ba31c7e465126ee2123fe3f2305d638e7827681e7cf6c83d5e",
			Command: []string{"sh", "-c", killSandboxScript},
			Env:     []corev1.EnvVar{{Name: "ACTOR_UID", Value: actorUID}},
			// the sandbox processes run unconfined, and the default AppArmor
			// profile, enforced on GKE, may not signal them
			SecurityContext: &corev1.SecurityContext{
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
			if st.Name != name || st.State.Terminated == nil {
				continue
			}
			if code := st.State.Terminated.ExitCode; code != 0 {
				t.Fatalf("killing the sandbox of actor %s exited %d: no process named it", actorUID, code)
			}
			t.Logf("Killed the sandbox of actor %s on %s/%s", actorUID, namespace, podName)
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out killing the sandbox of actor %s", actorUID)
}
