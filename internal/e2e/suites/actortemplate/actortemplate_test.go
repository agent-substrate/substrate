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

// Package actortemplate exercises the control-plane-native ActorTemplate /
// ActorTemplateVersion path end-to-end (issue #477): create AT+ATV via the
// Substrate API, wait for the in-ateapi builder to produce the golden
// snapshot, run counter actors from the version (pinned and via the
// template default), and verify the deletion invariants — the same coverage
// the demo suite has for the CRD path.
//
// Like the demo suite, it copies digest-resolved images from the deployed
// counter demo (ate-demo-counter/counter, overridable via
// E2E_TEMPLATE_NAMESPACE / E2E_TEMPLATE_NAME), so deploy the demo first:
// ./hack/install-ate.sh --deploy-demo-counter
package actortemplate

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const defaultVersionMaskPath = "spec.default_version_on_create"

// sourceDemo reads the deployed counter demo's WorkerPool and ActorTemplate
// CRDs, the source of digest-resolved images and the sandbox class.
func sourceDemo(ctx context.Context, t *testing.T, clients *e2e.Clients) (*v1alpha1.WorkerPool, *v1alpha1.ActorTemplate) {
	t.Helper()
	srcNS := "ate-demo-counter"
	if v := os.Getenv("E2E_TEMPLATE_NAMESPACE"); v != "" {
		srcNS = v
	}
	srcName := "counter"
	if v := os.Getenv("E2E_TEMPLATE_NAME"); v != "" {
		srcName = v
	}
	wp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get source WorkerPool %s/%s (deploy the counter demo first): %v", srcNS, srcName, err)
	}
	at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get source ActorTemplate %s/%s: %v", srcNS, srcName, err)
	}
	return wp, at
}

// containersToProto projects the source CRD containers onto the
// ActorTemplateVersion spec shape (literal env only).
func containersToProto(containers []v1alpha1.Container) []*ateapipb.Container {
	var out []*ateapipb.Container
	for _, c := range containers {
		pc := &ateapipb.Container{
			Name:    c.Name,
			Image:   c.Image,
			Command: c.Command,
			Args:    c.Args,
		}
		for _, env := range c.Env {
			if env.Value != nil {
				pc.Env = append(pc.Env, &ateapipb.EnvVar{
					Name:   env.Name,
					Source: &ateapipb.EnvVar_Value{Value: *env.Value},
				})
			}
		}
		if c.Readyz != nil && c.Readyz.HTTPGet != nil {
			pc.Readyz = &ateapipb.ContainerReadyz{
				TimeoutSeconds: c.Readyz.TimeoutSeconds,
				HttpGet: &ateapipb.HTTPGetAction{
					Path: c.Readyz.HTTPGet.Path,
					Port: c.Readyz.HTTPGet.Port,
				},
			}
		}
		for _, m := range c.VolumeMounts {
			pc.VolumeMounts = append(pc.VolumeMounts, &ateapipb.VolumeMount{Name: m.Name, MountPath: m.MountPath})
		}
		out = append(out, pc)
	}
	return out
}

func volumesToProto(volumes []v1alpha1.Volume) []*ateapipb.Volume {
	var out []*ateapipb.Volume
	for _, v := range volumes {
		pv := &ateapipb.Volume{Name: v.Name}
		switch {
		case v.DurableDir != nil:
			pv.Source = &ateapipb.Volume_DurableDir{DurableDir: &ateapipb.DurableDirVolumeSource{}}
		case v.ExternalVolumeTemplate != nil:
			pv.Source = &ateapipb.Volume_ExternalVolumeTemplate{ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				Capacity:         v.ExternalVolumeTemplate.Capacity.String(),
				StorageClassName: v.ExternalVolumeTemplate.StorageClassName,
			}}
		}
		out = append(out, pv)
	}
	return out
}

// defaultGvisorSandboxConfigName is the SandboxConfig installed by
// manifests/ate-install/sandboxconfig-gvisor.yaml.
const defaultGvisorSandboxConfigName = "gvisor-default"

func sandboxConfigToProto(class v1alpha1.SandboxClass, configName string) *ateapipb.SandboxConfig {
	protoClass := ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR
	if class == v1alpha1.SandboxClassMicroVM {
		protoClass = ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM
	}
	// The gvisor demo WorkerPool names no SandboxConfig; the native ATV spec
	// requires an explicit name.
	if configName == "" {
		configName = defaultGvisorSandboxConfigName
	}
	return &ateapipb.SandboxConfig{SandboxClass: protoClass, ConfigName: configName}
}

// nativeTemplate is one AT + ATV pair created for a test, with cleanup of
// the global-scoped resources registered on t.
type nativeTemplate struct {
	template string
	version  string
}

// createNativeTemplate creates a per-test WorkerPool CRD plus an
// ActorTemplate and one ActorTemplateVersion via the Substrate API. The
// template's worker selector pins it to this test's pool so no other suite's
// actors land on it (and vice versa).
func createNativeTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace) *nativeTemplate {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	srcWP, srcAT := sourceDemo(ctx, t, clients)

	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "counter-atv",
			Namespace: nsObj.Name,
			Labels:    map[string]string{"demo": nsObj.Name},
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          5,
			AteomImage:        srcWP.Spec.AteomImage,
			SandboxClass:      srcWP.Spec.SandboxClass,
			SandboxConfigName: srcWP.Spec.SandboxConfigName,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	// AT/ATV are global-scoped; derive globally-unique names from the test
	// namespace and clean them up explicitly — namespace GC won't.
	nt := &nativeTemplate{template: nsObj.Name, version: nsObj.Name + "-v1"}
	if _, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec: &ateapipb.ActorTemplateSpec{
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"demo": nsObj.Name}},
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	if _, err := clients.SubstrateAPI.CreateActorTemplateVersion(ctx, &ateapipb.CreateActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ActorTemplateVersion{
			Metadata:      &ateapipb.ResourceMetadata{Name: nt.version},
			ActorTemplate: &ateapipb.ObjectRef{Name: nt.template},
			Spec: &ateapipb.ActorTemplateVersionSpec{
				PauseImage: srcAT.Spec.PauseImage,
				Containers: containersToProto(srcAT.Spec.Containers),
				Volumes:    volumesToProto(srcAT.Spec.Volumes),
				SnapshotsConfig: &ateapipb.SnapshotsConfig{
					OnPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
					OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
					StorageLocation: "gs://" + env["BUCKET_NAME"] + "/ate-e2e-atv/" + nsObj.Name + "/",
				},
				SandboxConfig: sandboxConfigToProto(srcAT.Spec.SandboxClass, srcWP.Spec.SandboxConfigName),
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplateVersion failed: %v", err)
	}

	t.Cleanup(func() { cleanupNativeTemplate(t, clients, nt) })
	return nt
}

// cleanupNativeTemplate removes the global AT/ATV pair, tolerating partial
// state: clear the default pin, delete a mid-build golden actor if one is
// still around, then delete version and template.
func cleanupNativeTemplate(t *testing.T, clients *e2e.Clients, nt *nativeTemplate) {
	ctx := context.Background()
	_, _ = clients.SubstrateAPI.UpdateActorTemplate(ctx, &ateapipb.UpdateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec:     &ateapipb.ActorTemplateSpec{DefaultVersionOnCreate: nil},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{defaultVersionMaskPath}},
	})
	if atv, err := clients.SubstrateAPI.GetActorTemplateVersion(ctx, &ateapipb.GetActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ObjectRef{Name: nt.version},
	}); err == nil {
		if golden := atv.GetStatus().GetGoldenActor(); golden != nil {
			_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: golden})
		}
	}
	if _, err := clients.SubstrateAPI.DeleteActorTemplateVersion(ctx, &ateapipb.DeleteActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ObjectRef{Name: nt.version},
	}); err != nil && status.Code(err) != codes.NotFound {
		t.Logf("cleanup: DeleteActorTemplateVersion(%s): %v", nt.version, err)
	}
	if _, err := clients.SubstrateAPI.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Name: nt.template},
	}); err != nil && status.Code(err) != codes.NotFound {
		t.Logf("cleanup: DeleteActorTemplate(%s): %v", nt.template, err)
	}
}

// waitForVersionReady polls until the builder marks the version READY,
// failing fast on FAILED with the builder's message.
func waitForVersionReady(ctx context.Context, t *testing.T, clients *e2e.Clients, version string) *ateapipb.ActorTemplateVersion {
	t.Helper()
	timeout := 90 * time.Second
	if v := os.Getenv("E2E_TEMPLATE_READY_TIMEOUT"); v != "" {
		d, perr := time.ParseDuration(v)
		if perr != nil {
			t.Fatalf("invalid E2E_TEMPLATE_READY_TIMEOUT %q: %v", v, perr)
		}
		timeout = d
	}
	deadline := time.Now().Add(timeout)
	var last ateapipb.ActorTemplateVersionStatus_State
	for time.Now().Before(deadline) {
		atv, err := clients.SubstrateAPI.GetActorTemplateVersion(ctx, &ateapipb.GetActorTemplateVersionRequest{
			ActorTemplateVersion: &ateapipb.ObjectRef{Name: version},
		})
		if err == nil {
			last = atv.GetStatus().GetState()
			switch last {
			case ateapipb.ActorTemplateVersionStatus_STATE_READY:
				t.Logf("ActorTemplateVersion %s is READY with golden snapshot %q", version, atv.GetStatus().GetGoldenSnapshot().GetName())
				return atv
			case ateapipb.ActorTemplateVersionStatus_STATE_FAILED:
				t.Fatalf("ActorTemplateVersion %s FAILED: %s", version, atv.GetStatus().GetMessage())
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out after %v waiting for ActorTemplateVersion %s to be READY (last state: %s)", timeout, version, last)
	return nil
}

func createAtespace(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateAtespace(%s) failed: %v", name, err)
	}
}

func waitForActorStatus(ctx context.Context, t *testing.T, clients *e2e.Clients, actorRef resources.ActorRef, expected ateapipb.Actor_Status) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef.ToObjectRef()})
		if err == nil && resp.GetStatus() == expected {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %s to reach %v", actorRef, expected)
}

func validateCounterResponse(t *testing.T, resp, stage string, wantMemory, wantFile int) {
	t.Helper()
	if !strings.Contains(resp, fmt.Sprintf("preserved memory count: %d", wantMemory)) {
		t.Errorf("[%s] expected memory count %d, got response: %s", stage, wantMemory, resp)
	}
	if !strings.Contains(resp, fmt.Sprintf("preserved file counter: %d", wantFile)) {
		t.Errorf("[%s] expected file count %d, got response: %s", stage, wantFile, resp)
	}
}

// callActor sends one HTTP request to the actor through an atenet-router
// pod, retrying while routing converges. Ported from the demo suite.
func callActor(t *testing.T, actorRef resources.ActorRef) (string, error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := callActorOnce(t, actorRef)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}
	return "", fmt.Errorf("timed out waiting for actor response: %w", lastErr)
}

func callActorOnce(t *testing.T, actorRef resources.ActorRef) (string, error) {
	t.Helper()
	clients := e2e.GetClients()

	svc, err := clients.K8s.CoreV1().Services("ate-system").Get(context.Background(), "atenet-router", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get atenet-router service: %w", err)
	}
	selector := labels.SelectorFromSet(svc.Spec.Selector).String()
	pods, err := clients.K8s.CoreV1().Pods("ate-system").List(context.Background(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("failed to list atenet-router pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no atenet-router pods found")
	}
	targetPod := pods.Items[0]

	config, err := ateclient.LoadConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		return "", fmt.Errorf("failed to load kubeconfig: %w", err)
	}
	reqConfig := clients.K8s.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(targetPod.Namespace).
		Name(targetPod.Name).
		SubResource("portforward")
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return "", fmt.Errorf("failed to create SPDY transport: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, reqConfig.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	defer close(stopCh)
	fw, err := portforward.New(dialer, []string{"0:8080"}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return "", fmt.Errorf("failed to create port forwarder: %w", err)
	}
	errCh := make(chan error, 1)
	go func() {
		if err := fw.ForwardPorts(); err != nil {
			errCh <- err
		}
	}()
	select {
	case <-readyCh:
	case err := <-errCh:
		return "", fmt.Errorf("port forwarding failed: %w", err)
	case <-time.After(10 * time.Second):
		return "", fmt.Errorf("timeout waiting for port-forward")
	}
	forwardedPorts, err := fw.GetPorts()
	if err != nil || len(forwardedPorts) == 0 {
		return "", fmt.Errorf("failed to get forwarded ports: %w", err)
	}

	reqHTTP, err := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d", forwardedPorts[0].Local), nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	reqHTTP.Host = actorRef.DNSName()
	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(reqHTTP)
	if err != nil {
		return "", fmt.Errorf("failed to do request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}
	return string(body), nil
}

// withVersionEnv appends a VERSION env var to every container, so the
// counter's response identifies which version served it.
func withVersionEnv(containers []*ateapipb.Container, version string) []*ateapipb.Container {
	for _, c := range containers {
		c.Env = append(c.Env, &ateapipb.EnvVar{Name: "VERSION", Source: &ateapipb.EnvVar_Value{Value: version}})
	}
	return containers
}

// createUpgradeTemplate creates a WorkerPool, an ActorTemplate, and two
// versions differing only by their VERSION env var, committing DATA
// snapshots: an upgrade restore is data-only by design, so this template
// shape shows exactly what the upgrade preserves (durable files) and what it
// restarts (memory).
func createUpgradeTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace) (nt *nativeTemplate, v2 string) {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	srcWP, srcAT := sourceDemo(ctx, t, clients)

	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "counter-upgrade",
			Namespace: nsObj.Name,
			Labels:    map[string]string{"demo": nsObj.Name},
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          5,
			AteomImage:        srcWP.Spec.AteomImage,
			SandboxClass:      srcWP.Spec.SandboxClass,
			SandboxConfigName: srcWP.Spec.SandboxConfigName,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	nt = &nativeTemplate{template: nsObj.Name, version: nsObj.Name + "-v1"}
	v2 = nsObj.Name + "-v2"
	if _, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec: &ateapipb.ActorTemplateSpec{
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"demo": nsObj.Name}},
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	// Registered before the v2 cleanup below, so (LIFO) v2 is deleted first
	// and the template delete does not trip the versions-exist invariant.
	t.Cleanup(func() { cleanupNativeTemplate(t, clients, nt) })

	for _, v := range []struct{ name, label string }{{nt.version, "v1"}, {v2, "v2"}} {
		if _, err := clients.SubstrateAPI.CreateActorTemplateVersion(ctx, &ateapipb.CreateActorTemplateVersionRequest{
			ActorTemplateVersion: &ateapipb.ActorTemplateVersion{
				Metadata:      &ateapipb.ResourceMetadata{Name: v.name},
				ActorTemplate: &ateapipb.ObjectRef{Name: nt.template},
				Spec: &ateapipb.ActorTemplateVersionSpec{
					PauseImage: srcAT.Spec.PauseImage,
					Containers: withVersionEnv(containersToProto(srcAT.Spec.Containers), v.label),
					Volumes:    volumesToProto(srcAT.Spec.Volumes),
					SnapshotsConfig: &ateapipb.SnapshotsConfig{
						OnPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
						OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
						StorageLocation: "gs://" + env["BUCKET_NAME"] + "/ate-e2e-upgrade/" + nsObj.Name + "/",
					},
					SandboxConfig: sandboxConfigToProto(srcAT.Spec.SandboxClass, srcWP.Spec.SandboxConfigName),
				},
			},
		}); err != nil {
			t.Fatalf("CreateActorTemplateVersion(%s) failed: %v", v.name, err)
		}
	}
	t.Cleanup(func() {
		ctx := context.Background()
		if atv, err := clients.SubstrateAPI.GetActorTemplateVersion(ctx, &ateapipb.GetActorTemplateVersionRequest{
			ActorTemplateVersion: &ateapipb.ObjectRef{Name: v2},
		}); err == nil {
			if golden := atv.GetStatus().GetGoldenActor(); golden != nil {
				_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: golden})
			}
		}
		if _, err := clients.SubstrateAPI.DeleteActorTemplateVersion(ctx, &ateapipb.DeleteActorTemplateVersionRequest{
			ActorTemplateVersion: &ateapipb.ObjectRef{Name: v2},
		}); err != nil && status.Code(err) != codes.NotFound {
			t.Logf("cleanup: DeleteActorTemplateVersion(%s): %v", v2, err)
		}
	})
	return nt, v2
}

func validateVersionedResponse(t *testing.T, resp, stage, wantVersion string, wantMemory, wantFile int) {
	t.Helper()
	validateCounterResponse(t, resp, stage, wantMemory, wantFile)
	if !strings.Contains(resp, "version: "+wantVersion) {
		t.Errorf("[%s] expected version %s, got response: %s", stage, wantVersion, resp)
	}
}

// TestActorUpgradeOnResume drives the M2 upgrade primitive end to end:
// resume with a target version re-pins the actor and restores its durable
// data under the new version's spec (memory restarts — DATA commits cold
// boot), a running actor cannot be re-pinned, and reverting the pin via
// UpdateActor plus a plain resume rolls back. Requires a deployed counter
// demo image that echoes the VERSION env var.
//
// Two golden snapshot builds run back to back; on a cold cluster raise
// E2E_TEMPLATE_READY_TIMEOUT accordingly.
func TestActorUpgradeOnResume(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	nt, v2 := createUpgradeTemplate(ctx, t, clients, nsObj)
	waitForVersionReady(ctx, t, clients, nt.version)
	waitForVersionReady(ctx, t, clients, v2)

	atespace := nsObj.Name
	createAtespace(ctx, t, clients, atespace)

	actorRef := resources.ActorRef{Atespace: atespace, Name: "counter-upgrade"}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorRef.Name},
		ActorTemplate:        nt.template,
		ActorTemplateVersion: nt.version,
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	defer clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: actorRef.ToObjectRef()})

	// v1 serves traffic.
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorRef, ateapipb.Actor_STATUS_RUNNING)
	resp, err := callActor(t, actorRef)
	if err != nil {
		t.Fatalf("callActor failed: %v", err)
	}
	validateVersionedResponse(t, resp, "on v1", "v1", 1, 1)

	// A running actor cannot be re-pinned.
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                actorRef.ToObjectRef(),
		ActorTemplateVersion: v2,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResumeActor(version) on RUNNING actor = %v, want FailedPrecondition", err)
	}

	// Upgrade: suspend, then resume onto v2. The durable file counter
	// carries over; the memory count restarts with the cold boot.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorRef, ateapipb.Actor_STATUS_SUSPENDED)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                actorRef.ToObjectRef(),
		ActorTemplateVersion: v2,
	}); err != nil {
		t.Fatalf("ResumeActor(upgrade to %s) failed: %v", v2, err)
	}
	waitForActorStatus(ctx, t, clients, actorRef, ateapipb.Actor_STATUS_RUNNING)
	upgraded, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef.ToObjectRef()})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := upgraded.GetActorTemplateVersion(); got != v2 {
		t.Errorf("actor pin after upgrade = %q, want %q", got, v2)
	}
	resp, err = callActor(t, actorRef)
	if err != nil {
		t.Fatalf("callActor after upgrade failed: %v", err)
	}
	validateVersionedResponse(t, resp, "after upgrade", "v2", 1, 2)

	// Rollback via the UpdateActor primitive: revert the pin, plain resume.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("SuspendActor before rollback failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorRef, ateapipb.Actor_STATUS_SUSPENDED)
	if _, err := clients.SubstrateAPI.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:             &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorRef.Name},
			ActorTemplateVersion: nt.version,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"actor_template_version"}},
	}); err != nil {
		t.Fatalf("UpdateActor(rollback pin) failed: %v", err)
	}
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor after rollback failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorRef, ateapipb.Actor_STATUS_RUNNING)
	resp, err = callActor(t, actorRef)
	if err != nil {
		t.Fatalf("callActor after rollback failed: %v", err)
	}
	validateVersionedResponse(t, resp, "after rollback", "v1", 1, 3)

	// Suspend before delete so the actor is deletable.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("final SuspendActor failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorRef, ateapipb.Actor_STATUS_SUSPENDED)
}

// TestActorTemplateVersionBuildAndLifecycle drives the whole native path:
// AT+ATV creation, golden-snapshot build, actor creation pinned and via the
// template default, live traffic, and suspend/resume + pause/resume state
// preservation.
func TestActorTemplateVersionBuildAndLifecycle(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	nt := createNativeTemplate(ctx, t, clients, nsObj)
	ready := waitForVersionReady(ctx, t, clients, nt.version)

	// The golden snapshot must resolve to a stored ActorSnapshot in the
	// reserved golden atespace.
	golden := ready.GetStatus().GetGoldenSnapshot()
	if golden.GetAtespace() != resources.GoldenActorAtespace {
		t.Errorf("golden snapshot atespace = %q, want %s", golden.GetAtespace(), resources.GoldenActorAtespace)
	}
	if _, err := clients.SubstrateAPI.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{
		Snapshot: &ateapipb.ActorSnapshotRef{Reference: &ateapipb.ActorSnapshotRef_Snapshot{Snapshot: golden}},
	}); err != nil {
		t.Errorf("golden snapshot %v not resolvable: %v", golden, err)
	}

	atespace := nsObj.Name
	createAtespace(ctx, t, clients, atespace)

	// Pinned create.
	pinnedRef := resources.ActorRef{Atespace: atespace, Name: "counter-pinned"}
	pinned, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: atespace, Name: pinnedRef.Name},
		ActorTemplate:        nt.template,
		ActorTemplateVersion: nt.version,
	}})
	if err != nil {
		t.Fatalf("CreateActor(pinned) failed: %v", err)
	}
	defer clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: pinnedRef.ToObjectRef()})
	if pinned.GetActorTemplateNamespace() != "" || pinned.GetActorTemplateName() != "" {
		t.Errorf("native actor carries CRD identity (%q, %q), want empty", pinned.GetActorTemplateNamespace(), pinned.GetActorTemplateName())
	}

	// Unpinned create resolves the template default once one is set.
	if _, err := clients.SubstrateAPI.UpdateActorTemplate(ctx, &ateapipb.UpdateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec:     &ateapipb.ActorTemplateSpec{DefaultVersionOnCreate: &ateapipb.ObjectRef{Name: nt.version}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{defaultVersionMaskPath}},
	}); err != nil {
		t.Fatalf("UpdateActorTemplate(default) failed: %v", err)
	}
	defaultedRef := resources.ActorRef{Atespace: atespace, Name: "counter-defaulted"}
	defaulted, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: defaultedRef.Name},
		ActorTemplate: nt.template,
	}})
	if err != nil {
		t.Fatalf("CreateActor(via default) failed: %v", err)
	}
	defer clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: defaultedRef.ToObjectRef()})
	if defaulted.GetActorTemplateVersion() != nt.version {
		t.Errorf("default resolution stored version %q, want %q", defaulted.GetActorTemplateVersion(), nt.version)
	}

	// Resume from the golden snapshot and serve traffic.
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: pinnedRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, pinnedRef, ateapipb.Actor_STATUS_RUNNING)
	resp, err := callActor(t, pinnedRef)
	if err != nil {
		t.Fatalf("callActor failed: %v", err)
	}
	validateCounterResponse(t, resp, "initial", 1, 1)

	// Suspend/resume: FULL commit snapshots preserve memory and files.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: pinnedRef.ToObjectRef()}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, pinnedRef, ateapipb.Actor_STATUS_SUSPENDED)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: pinnedRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor after suspend failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, pinnedRef, ateapipb.Actor_STATUS_RUNNING)
	resp, err = callActor(t, pinnedRef)
	if err != nil {
		t.Fatalf("callActor after suspend/resume failed: %v", err)
	}
	validateCounterResponse(t, resp, "after suspend/resume", 2, 2)

	// Pause/resume preserves the local snapshot state the same way.
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: pinnedRef.ToObjectRef()}); err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, pinnedRef, ateapipb.Actor_STATUS_PAUSED)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: pinnedRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor after pause failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, pinnedRef, ateapipb.Actor_STATUS_RUNNING)
	resp, err = callActor(t, pinnedRef)
	if err != nil {
		t.Fatalf("callActor after pause/resume failed: %v", err)
	}
	validateCounterResponse(t, resp, "after pause/resume", 3, 3)

	// Suspend before delete so the actor is deletable.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: pinnedRef.ToObjectRef()}); err != nil {
		t.Fatalf("final SuspendActor failed: %v", err)
	}
	waitForActorStatus(ctx, t, clients, pinnedRef, ateapipb.Actor_STATUS_SUSPENDED)
}

// TestActorTemplateVersionGating verifies CreateActor's native-path
// preconditions: no default, non-READY version, missing parents.
func TestActorTemplateVersionGating(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	atespace := nsObj.Name
	createAtespace(ctx, t, clients, atespace)

	// A template with a selector matching no pool: its version never
	// becomes READY within this test.
	nt := &nativeTemplate{template: nsObj.Name + "-gate", version: nsObj.Name + "-gate-v1"}
	if _, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec: &ateapipb.ActorTemplateSpec{
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"demo": nsObj.Name + "-nopool"}},
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	t.Cleanup(func() { cleanupNativeTemplate(t, clients, nt) })

	// No pin and no default.
	_, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: "no-default"},
		ActorTemplate: nt.template,
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor without default = %v, want FailedPrecondition", err)
	}

	// Version exists but is not READY.
	_, srcAT := sourceDemo(ctx, t, clients)
	if _, err := clients.SubstrateAPI.CreateActorTemplateVersion(ctx, &ateapipb.CreateActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ActorTemplateVersion{
			Metadata:      &ateapipb.ResourceMetadata{Name: nt.version},
			ActorTemplate: &ateapipb.ObjectRef{Name: nt.template},
			Spec: &ateapipb.ActorTemplateVersionSpec{
				PauseImage: srcAT.Spec.PauseImage,
				SnapshotsConfig: &ateapipb.SnapshotsConfig{
					StorageLocation: "gs://e2e-fake-bucket/ate-e2e-atv/" + nsObj.Name + "/",
				},
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplateVersion failed: %v", err)
	}
	_, err = clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: atespace, Name: "not-ready"},
		ActorTemplate:        nt.template,
		ActorTemplateVersion: nt.version,
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor with non-READY version = %v, want FailedPrecondition", err)
	}

	// Missing template, and a version under a nonexistent template.
	_, err = clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: "no-template"},
		ActorTemplate: nsObj.Name + "-does-not-exist",
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor with missing template = %v, want FailedPrecondition", err)
	}
	_, err = clients.SubstrateAPI.CreateActorTemplateVersion(ctx, &ateapipb.CreateActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ActorTemplateVersion{
			Metadata:      &ateapipb.ResourceMetadata{Name: nsObj.Name + "-orphan-v1"},
			ActorTemplate: &ateapipb.ObjectRef{Name: nsObj.Name + "-does-not-exist"},
			Spec: &ateapipb.ActorTemplateVersionSpec{
				PauseImage:      srcAT.Spec.PauseImage,
				SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://e2e-fake-bucket/orphan/"},
			},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActorTemplateVersion under missing template = %v, want FailedPrecondition", err)
	}
}

// TestActorTemplateDeletionInvariants verifies the deletion ordering rules:
// no template delete while versions exist, no version delete while it is the
// default.
func TestActorTemplateDeletionInvariants(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	_, srcAT := sourceDemo(ctx, t, clients)
	nt := &nativeTemplate{template: nsObj.Name + "-del", version: nsObj.Name + "-del-v1"}
	if _, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec: &ateapipb.ActorTemplateSpec{
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"demo": nsObj.Name + "-nopool"}},
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	t.Cleanup(func() { cleanupNativeTemplate(t, clients, nt) })
	if _, err := clients.SubstrateAPI.CreateActorTemplateVersion(ctx, &ateapipb.CreateActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ActorTemplateVersion{
			Metadata:      &ateapipb.ResourceMetadata{Name: nt.version},
			ActorTemplate: &ateapipb.ObjectRef{Name: nt.template},
			Spec: &ateapipb.ActorTemplateVersionSpec{
				PauseImage:      srcAT.Spec.PauseImage,
				SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://e2e-fake-bucket/ate-e2e-del/" + nsObj.Name + "/"},
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplateVersion failed: %v", err)
	}

	// Template delete is blocked while a version exists.
	if _, err := clients.SubstrateAPI.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Name: nt.template},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("DeleteActorTemplate with versions = %v, want FailedPrecondition", err)
	}

	// Version delete is blocked while it is the default.
	if _, err := clients.SubstrateAPI.UpdateActorTemplate(ctx, &ateapipb.UpdateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec:     &ateapipb.ActorTemplateSpec{DefaultVersionOnCreate: &ateapipb.ObjectRef{Name: nt.version}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{defaultVersionMaskPath}},
	}); err != nil {
		t.Fatalf("UpdateActorTemplate(default) failed: %v", err)
	}
	if _, err := clients.SubstrateAPI.DeleteActorTemplateVersion(ctx, &ateapipb.DeleteActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ObjectRef{Name: nt.version},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("DeleteActorTemplateVersion while default = %v, want FailedPrecondition", err)
	}

	// Clearing the default unblocks the version, then the template.
	if _, err := clients.SubstrateAPI.UpdateActorTemplate(ctx, &ateapipb.UpdateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: nt.template},
			Spec:     &ateapipb.ActorTemplateSpec{DefaultVersionOnCreate: nil},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{defaultVersionMaskPath}},
	}); err != nil {
		t.Fatalf("UpdateActorTemplate(clear default) failed: %v", err)
	}
	if _, err := clients.SubstrateAPI.DeleteActorTemplateVersion(ctx, &ateapipb.DeleteActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ObjectRef{Name: nt.version},
	}); err != nil {
		t.Fatalf("DeleteActorTemplateVersion after clearing default failed: %v", err)
	}
	if _, err := clients.SubstrateAPI.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Name: nt.template},
	}); err != nil {
		t.Fatalf("DeleteActorTemplate after versions removed failed: %v", err)
	}
}
