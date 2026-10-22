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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// testserverImage resolves the e2e testserver image, whose kill and procstatus
// subcommands run inside the worker's PID namespace.
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
	// kills the runsc sandbox and gofer, or cloud-hypervisor and virtiofsd:
	// each names the actor UID in its command line
	runInWorkerPIDNamespace(ctx, t, clients, namespace, podName, "kill-sandbox", []string{"kill", "--cmdline-contains", actorUID})
	t.Logf("Killed the sandbox of actor %s on %s/%s", actorUID, namespace, podName)
}

// WorkerProcessStatus is the kernel's view of one process on a worker: its
// capability masks, no_new_privs and seccomp state, read from
// /proc/<pid>/status. It mirrors the testserver procstatus output.
type WorkerProcessStatus struct {
	PID            int    `json:"pid"`
	Cmdline        string `json:"cmdline"`
	NSPID          []int  `json:"nspid"`
	CapPrm         uint64 `json:"capPrm"`
	CapEff         uint64 `json:"capEff"`
	CapBnd         uint64 `json:"capBnd"`
	CapAmb         uint64 `json:"capAmb"`
	NoNewPrivs     int    `json:"noNewPrivs"`
	Seccomp        int    `json:"seccomp"`
	SeccompFilters int    `json:"seccompFilters"`
}

// ReadWorkerProcessStatus reports the worker process (PID 1 of the worker
// container) and every process in its PID namespace whose command line contains
// one of cmdlineContains, as the kernel enforces them. The rendered PodSpec says
// what was asked for; this says what the container runtime actually granted.
func ReadWorkerProcessStatus(ctx context.Context, t *testing.T, clients *Clients, namespace, podName string, cmdlineContains ...string) []WorkerProcessStatus {
	t.Helper()
	args := []string{"procstatus"}
	for _, c := range cmdlineContains {
		args = append(args, "--cmdline-contains", c)
	}
	logs := runInWorkerPIDNamespace(ctx, t, clients, namespace, podName, "procstatus", args)
	var rows []WorkerProcessStatus
	if err := json.Unmarshal([]byte(logs), &rows); err != nil {
		t.Fatalf("parsing procstatus output from %s/%s: %v\n%s", namespace, podName, err, logs)
	}
	return rows
}

// runInWorkerPIDNamespace runs one testserver subcommand in an ephemeral
// container that shares the worker container's PID namespace, waits for it to
// exit cleanly and returns what it wrote to stdout.
func runInWorkerPIDNamespace(ctx context.Context, t *testing.T, clients *Clients, namespace, podName, prefix string, args []string) string {
	t.Helper()
	pods := clients.K8s.CoreV1().Pods(namespace)
	pod, err := pods.Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting worker pod %s/%s: %v", namespace, podName, err)
	}
	name := fmt.Sprintf("%s-%d", prefix, len(pod.Spec.EphemeralContainers))
	pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, corev1.EphemeralContainer{
		TargetContainerName: "worker",
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name:  name,
			Image: testserverImage(t),
			Args:  args,
			// root, like the sandbox processes, which also run unconfined, and
			// the default AppArmor profile, enforced on GKE, may not signal
			// them or read their /proc entries
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
				t.Fatalf("%s container on %s/%s cannot start: %s: %s", name, namespace, podName, w.Reason, w.Message)
			}
			if st.State.Terminated == nil {
				continue
			}
			logs := ephemeralContainerLogs(ctx, t, clients, namespace, podName, name)
			if code := st.State.Terminated.ExitCode; code != 0 {
				t.Fatalf("%s container on %s/%s exited %d\n%s", name, namespace, podName, code, logs)
			}
			return logs
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for the %s container on %s/%s", name, namespace, podName)
	return ""
}

func ephemeralContainerLogs(ctx context.Context, t *testing.T, clients *Clients, namespace, podName, container string) string {
	t.Helper()
	stream, err := clients.K8s.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Container: container}).Stream(ctx)
	if err != nil {
		t.Fatalf("reading %s logs from %s/%s: %v", container, namespace, podName, err)
	}
	defer stream.Close()
	out, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("reading %s logs from %s/%s: %v", container, namespace, podName, err)
	}
	return string(out)
}
