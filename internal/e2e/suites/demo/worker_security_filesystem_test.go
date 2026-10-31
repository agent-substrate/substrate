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

package demo

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/e2e/procstatus"
	"go.ate.dev/substrate/internal/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The fixed worker capability sets, as the controller renders them. The e2e
// test restates them rather than importing the controller so that a change to
// the defaults has to be made deliberately in both places.
var (
	gvisorWorkerCapabilities  = []corev1.Capability{"NET_ADMIN", "SYS_ADMIN", "SETUID", "SETGID", "DAC_OVERRIDE", "FOWNER", "CHOWN", "NET_RAW", "SETFCAP"}
	microVMWorkerCapabilities = []corev1.Capability{"NET_ADMIN", "SYS_ADMIN", "SETUID", "SETGID", "DAC_OVERRIDE", "FOWNER", "CHOWN", "MKNOD", "SETFCAP", "FSETID", "SETPCAP"}
	// virtiofsd's default child set: what its serving child keeps in its
	// permitted and effective sets after it drops the rest. With file handles
	// off it does not add DAC_READ_SEARCH.
	virtiofsdChildCapabilities = []corev1.Capability{"CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "SETGID", "SETUID", "MKNOD", "SETFCAP"}
)

// capabilityBits are the Linux capability numbers (include/uapi/linux/capability.h)
// of every capability a worker set may name, so a set can be compared with the
// masks in /proc/<pid>/status.
var capabilityBits = map[corev1.Capability]uint{
	"CHOWN": 0, "DAC_OVERRIDE": 1, "DAC_READ_SEARCH": 2, "FOWNER": 3, "FSETID": 4, "KILL": 5,
	"SETGID": 6, "SETUID": 7, "SETPCAP": 8, "NET_BIND_SERVICE": 10, "NET_ADMIN": 12, "NET_RAW": 13,
	"SYS_CHROOT": 18, "SYS_PTRACE": 19, "SYS_ADMIN": 21, "MKNOD": 27, "AUDIT_WRITE": 29, "SETFCAP": 31,
}

func capabilityMask(caps []corev1.Capability) uint64 {
	var mask uint64
	for _, c := range caps {
		bit, ok := capabilityBits[c]
		if !ok {
			panic("unknown capability " + string(c))
		}
		mask |= 1 << bit
	}
	return mask
}

// assertWorkerProcessState checks what the kernel enforces on the running
// worker, not what the PodSpec requested: the worker's permitted, effective and
// bounding sets are exactly the fixed set, nothing is ambient, and no_new_privs
// is on. Each process that serves the guest must exist and run under its own
// seccomp filter. On gVisor these are runsc's gofer and sandbox. On microVM
// they are virtiofsd's serving child and cloud-hypervisor.
func assertWorkerProcessState(ctx context.Context, t *testing.T, clients *e2e.Clients, namespace, podName string, wantMask uint64) {
	t.Helper()
	// runsc sets these as the argv[0] of its gofer and sandbox
	names := []string{"runsc-gofer", "runsc-sandbox"}
	if e2e.IsMicroVM() {
		names = []string{"virtiofsd", "cloud-hypervisor"}
	}
	procs := e2e.ReadWorkerProcessStatus(ctx, t, clients, namespace, podName, names...)
	i := slices.IndexFunc(procs, func(p procstatus.Process) bool { return p.PID == 1 })
	if i < 0 {
		t.Fatalf("procstatus did not report the worker (PID 1): %+v", procs)
	}
	worker := procs[i]
	if worker.CapPrm != wantMask || worker.CapEff != wantMask || worker.CapBnd != wantMask || worker.CapAmb != 0 {
		t.Errorf("worker capabilities prm=%#x eff=%#x bnd=%#x amb=%#x, want all %#x and no ambient", worker.CapPrm, worker.CapEff, worker.CapBnd, worker.CapAmb, wantMask)
	}
	if worker.NoNewPrivs != 1 {
		t.Errorf("worker NoNewPrivs = %d, want 1", worker.NoNewPrivs)
	}
	if e2e.IsMicroVM() {
		assertVirtiofsdChild(t, procs)
		assertCloudHypervisor(t, procs)
		return
	}
	// runsc installs its filters with SECCOMP_FILTER_FLAG_TSYNC, so the main
	// thread's mode is every thread's
	for _, name := range names {
		for _, p := range processesNamed(t, procs, name) {
			if p.NoNewPrivs != 1 || p.Seccomp != 2 {
				t.Errorf("%s %d NoNewPrivs=%d Seccomp=%d, want 1 and 2", name, p.PID, p.NoNewPrivs, p.Seccomp)
			}
		}
	}
}

// processesNamed returns the processes called name, and fails the test if
// there are none.
func processesNamed(t *testing.T, procs []procstatus.Process, name string) []procstatus.Process {
	t.Helper()
	var named []procstatus.Process
	for _, p := range procs {
		if p.Name == name {
			named = append(named, p)
		}
	}
	if len(named) == 0 {
		t.Errorf("no %s process in the worker's PID namespace", name)
	}
	return named
}

// assertVirtiofsdChild checks virtiofsd's serving child. virtiofsd forks it
// into new namespaces and then only waits for it, so the child is the
// virtiofsd whose parent is also virtiofsd. The child keeps its eight
// capabilities in the permitted and effective sets and clears its bounding set
// with PR_CAPBSET_DROP, which libcap-ng only issues while CAP_SETPCAP is
// effective. A worker without SETPCAP leaves the child's bounding set at the
// worker's own.
func assertVirtiofsdChild(t *testing.T, procs []procstatus.Process) {
	t.Helper()
	virtiofsd := map[int]bool{}
	for _, p := range procs {
		if p.Name == "virtiofsd" {
			virtiofsd[p.PID] = true
		}
	}
	want := capabilityMask(virtiofsdChildCapabilities)
	children := 0
	for _, p := range procs {
		if !virtiofsd[p.PID] || !virtiofsd[p.PPID] {
			continue
		}
		children++
		if p.NoNewPrivs != 1 || p.Seccomp != 2 {
			t.Errorf("virtiofsd child %d NoNewPrivs=%d Seccomp=%d, want 1 and 2", p.PID, p.NoNewPrivs, p.Seccomp)
		}
		if p.CapPrm != want || p.CapEff != want || p.CapBnd != 0 {
			t.Errorf("virtiofsd child %d capabilities prm=%#x eff=%#x bnd=%#x, want prm and eff %#x and an empty bounding set", p.PID, p.CapPrm, p.CapEff, p.CapBnd, want)
		}
	}
	if children == 0 {
		t.Errorf("no virtiofsd serving child, a virtiofsd whose parent is also virtiofsd, in the worker's PID namespace")
	}
}

// assertCloudHypervisor checks that every thread cloud-hypervisor starts runs
// under seccomp. It installs a filter on each of those threads but never on
// its main thread, which only waits for the vmm thread to exit.
func assertCloudHypervisor(t *testing.T, procs []procstatus.Process) {
	t.Helper()
	for _, p := range processesNamed(t, procs, "cloud-hypervisor") {
		if p.NoNewPrivs != 1 {
			t.Errorf("cloud-hypervisor %d NoNewPrivs = %d, want 1", p.PID, p.NoNewPrivs)
		}
		vcpus := 0
		for _, thread := range p.Threads {
			if thread.TID == p.PID {
				continue
			}
			if strings.HasPrefix(thread.Name, "vcpu") {
				vcpus++
			}
			if thread.Seccomp != 2 {
				t.Errorf("cloud-hypervisor %d thread %d (%s) Seccomp = %d, want 2", p.PID, thread.TID, thread.Name, thread.Seccomp)
			}
		}
		// the vcpu threads run the guest; a VMM without any has no VM to check
		if vcpus == 0 {
			t.Errorf("cloud-hypervisor %d has no vcpu thread: %+v", p.PID, p.Threads)
		}
	}
}

// TestWorkerSecurityFilesystem verifies the guest operations and ownership
// restore that a root-only counter does not exercise, and that the kernel
// enforces the worker's fixed capability set while an actor runs.
func TestWorkerSecurityFilesystem(t *testing.T) {
	clients := e2e.GetClients()
	src := e2e.SubstrateCounterFixture()
	pool, err := clients.SubstrateK8s.AteV1().WorkerPools(src.PoolNamespace).Get(t.Context(), src.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	image := os.Getenv("E2E_WORKER_FILESYSTEM_PROBE_IMAGE")
	if image == "" {
		image = e2e.ResolveFixtureImage(t, "go.ate.dev/substrate/internal/e2e/fixtures/probe")
	}
	if !strings.Contains(image, "@sha256:") {
		t.Fatal("E2E_WORKER_FILESYSTEM_PROBE_IMAGE must be pinned by digest")
	}
	ctx := t.Context()
	ns := e2e.CreateNamespace(t)
	template := pool.Spec.Template.DeepCopy()
	if template == nil {
		template = &atev1.WorkerPoolPodTemplate{}
	}
	template.ServiceAccountName = nil
	actorName := "worker-filesystem-" + ns.Name
	at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, ns.Name, e2e.SubstrateTemplateOptions{
		Atespace: demoAtespace, Name: actorName, PoolName: "worker-filesystem", PoolReplicas: 1, PoolTemplate: template, Labels: map[string]string{"worker-filesystem": ns.Name},
		Modify: func(at *ateapi.ActorTemplate) {
			at.Containers = []*ateapi.Container{{
				Name: "probe", Image: image, Command: []string{"/ko-app/probe"},
				VolumeMounts: []*ateapi.VolumeMount{{Name: "data", MountPath: "/home/counter"}},
				WakeupProbe: &ateapi.WakeupProbe{
					HttpGet: &ateapi.HttpGetAction{Path: "/healthz", Port: 80}, TimeoutSeconds: 30,
				},
				SecurityContext: &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{
					Add: []string{"CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "SETUID", "SETGID", "MKNOD"},
				}},
			}}
		},
	})
	pods, err := clients.K8s.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: "ate.dev/worker-pool=worker-filesystem"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("worker pod count: %d, want 1", len(pods.Items))
	}
	worker := pods.Items[0]
	sc := worker.Spec.Containers[0].SecurityContext
	wantCaps := gvisorWorkerCapabilities
	if e2e.IsMicroVM() {
		wantCaps = microVMWorkerCapabilities
	}
	if sc == nil || sc.Capabilities == nil || !slices.Equal(sc.Capabilities.Add, wantCaps) || !slices.Equal(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatalf("worker security context = %+v, want add %v and drop [ALL]", sc, wantCaps)
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Fatal("worker privilege escalation was not disabled")
	}
	ref := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName}, ActorTemplate: e2e.TemplateRef(at)}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref})
	})
	for turn := 1; turn <= 3; turn++ {
		if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
			t.Fatal(err)
		}
		method, path := "GET", "/filesystem"
		if turn == 1 {
			method = "POST"
		}
		if e2e.IsMicroVM() {
			path += "?nonroot=1"
		}
		raw, err := callActorPath(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName}, method, path)
		if err != nil {
			t.Fatal(err)
		}
		assertWorkerFilesystem(t, raw)
		if turn == 1 {
			assertWorkerProcessState(ctx, t, clients, worker.Namespace, worker.Name, capabilityMask(wantCaps))
		}
		if turn == 1 {
			// the asynchronous mode keeps the snapshot on the worker's node,
			// which is what PauseActor used to do
			_, err = clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
				Actor:   ref,
				Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
			})
		} else {
			_, err = clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

func assertWorkerFilesystem(t *testing.T, raw string) {
	t.Helper()
	var response struct {
		Error string
		Files []struct {
			Path, Contents string
			UID, GID, Mode uint32
		}
	}
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != "" || len(response.Files) != 2 {
		t.Fatalf("guest filesystem operations: %s", raw)
	}
	for _, file := range response.Files {
		if file.Path != "/var/worker-security/owned" && file.Path != "/home/counter/worker-security/owned" {
			t.Fatalf("unexpected filesystem test path: %s", raw)
		}
		if file.UID != 1000 || file.GID != 1000 || file.Mode != 0o2755 || file.Contents != "worker filesystem state" {
			t.Fatalf("guest file ownership, mode or contents changed: %s", raw)
		}
	}
	if response.Files[0].Path == response.Files[1].Path {
		t.Fatalf("rootfs and durable-directory checks returned the same path: %s", raw)
	}
}
