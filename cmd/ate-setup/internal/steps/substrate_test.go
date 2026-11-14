// Copyright 2026 The Agent Substrate Authors
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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/resources"
)

// TestActorTemplateFromManifestDemos parses the protojson ActorTemplate
// manifests that only the shell installers deploy; the manifests of demos
// registered with ate-setup are covered by the demos package's render test.
// Substitutions mirror what the deploying script performs.
func TestActorTemplateFromManifestDemos(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}

	// The values benchmarking/workloads/deploy.sh substitutes.
	benchmarkValues := map[string]string{
		"${ACTOR_CPU_MILLICORES}": "1000",
		"${ACTOR_MEMORY_BYTES}":   "268435456",
		"${ACTOR_MEMORY}":         "256Mi",
		"${ATE_ENV_IMAGE}":        "gcr.io/example/ate-env-guest@sha256:1d38934ef1dac71fcb1f1286de734e06ab2e19747279bccbd37bfe69e0d5bc72",
		"${OTLP_ENDPOINT}":        "http://otel-collector.otel-system:4317",
	}

	tests := []struct {
		relPath string
		values  map[string]string
		want    resources.ActorTemplateRef
	}{
		{
			relPath: "demos/counter/counter-microvm-csi-test-template.yaml",
			want:    resources.ActorTemplateRef{Atespace: "ate-demo-counter-microvm-csi", Name: "counter-microvm-csi"},
		},
		{
			relPath: "demos/jupyter/jupyter-template.yaml.tmpl",
			want:    resources.ActorTemplateRef{Atespace: "ate-demo-jupyter", Name: "jupyter"},
		},
		{
			relPath: "demos/sandbox/manual-test-multi-template.yaml",
			want:    resources.ActorTemplateRef{Atespace: "ate-manual-test-multi", Name: "sandbox-template"},
		},
		{
			relPath: "benchmarking/workloads/manifests/ate-env-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "ate-env"},
		},
		{
			relPath: "benchmarking/workloads/manifests/sleep-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "sleep"},
		},
		{
			relPath: "benchmarking/workloads/manifests/glutton-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "glutton"},
		},
		{
			relPath: "benchmarking/workloads/manifests/glutton-durdir-data-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "glutton-durdir-data"},
		},
		{
			relPath: "benchmarking/workloads/manifests/glutton-durdir-full-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "glutton-durdir-full"},
		},
		{
			relPath: "benchmarking/workloads/manifests/usermem-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "usermem"},
		},
		{
			relPath: "benchmarking/workloads/manifests/kernelmem-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "kernelmem"},
		},
		{
			relPath: "benchmarking/nighthawk-egress/traffic-driver-template.yaml.tmpl",
			want:    resources.ActorTemplateRef{Atespace: "nighthawk", Name: "traffic-driver"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.relPath, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, tc.relPath))
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			manifest := string(data)
			for placeholder, value := range tc.values {
				manifest = strings.ReplaceAll(manifest, placeholder, value)
			}
			if i := strings.Index(manifest, "${"); i >= 0 {
				t.Fatalf("unsubstituted placeholder remains: %s", manifest[i:min(i+40, len(manifest))])
			}
			template, err := ActorTemplateFromManifest([]byte(manifest))
			if err != nil {
				t.Fatalf("ActorTemplateFromManifest: %v", err)
			}
			if got := resources.ActorTemplateRefFromActorTemplate(template); got != tc.want {
				t.Errorf("manifest names template %s, want %s", got, tc.want)
			}
			// The API rejects storageLocation without its preview gate.
			if got := template.GetSnapshotConfig().GetStorageLocation(); got != "" {
				t.Errorf("manifest sets storageLocation %q, want none", got)
			}
		})
	}
}

func TestActorTemplateFromManifestErrors(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
	}{
		{name: "empty", manifest: ""},
		{name: "not yaml", manifest: ":\t:"},
		// Strict parsing: a typo must fail rather than silently drop the field.
		{name: "unknown field", manifest: "metadata:\n  atespace: a\n  name: t\nsnapshotsConfig: {}\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ActorTemplateFromManifest([]byte(tc.manifest)); err == nil {
				t.Error("ActorTemplateFromManifest succeeded, want error")
			}
		})
	}
}

// failingControlClient fails every GetActorTemplate with a fixed error.
type failingControlClient struct {
	ateapi.ControlClient
	err error
}

func (f failingControlClient) GetActorTemplate(ctx context.Context, in *ateapi.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	return nil, f.err
}

// TestWaitActorTemplateGoldenTimeoutIncludesLastError: a wait that spent its
// budget on failing Gets must surface the RPC error, not just "timed out".
func TestWaitActorTemplateGoldenTimeoutIncludesLastError(t *testing.T) {
	getErr := errors.New("connection refused")
	client := &ateclient.Client{ControlClient: failingControlClient{err: getErr}}
	ref := resources.ActorTemplateRef{Atespace: "ate-demo", Name: "counter"}

	err := WaitActorTemplateGolden(context.Background(), client, ref, 0)
	if err == nil {
		t.Fatal("WaitActorTemplateGolden succeeded, want timeout error")
	}
	if !errors.Is(err, getErr) {
		t.Errorf("WaitActorTemplateGolden error = %v, want it to wrap %v", err, getErr)
	}
}

// disabledTemplateClient serves one ActorTemplate that opted out of the
// golden snapshot and has no golden status.
type disabledTemplateClient struct {
	ateapi.ControlClient
}

func (disabledTemplateClient) GetActorTemplate(_ context.Context, in *ateapi.GetActorTemplateRequest, _ ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	return &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: in.GetActorTemplate().GetAtespace(), Name: in.GetActorTemplate().GetName()},
		SnapshotConfig: &ateapi.SnapshotConfig{GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}},
	}, nil
}

// TestWaitActorTemplateGoldenDisabledTemplateReady: a template that opted out
// never gets a golden tag or an error, so the wait must treat it as ready
// instead of timing out.
func TestWaitActorTemplateGoldenDisabledTemplateReady(t *testing.T) {
	client := &ateclient.Client{ControlClient: disabledTemplateClient{}}
	ref := resources.ActorTemplateRef{Atespace: "ate-demo", Name: "counter"}

	if err := WaitActorTemplateGolden(context.Background(), client, ref, 0); err != nil {
		t.Fatalf("WaitActorTemplateGolden on a disabled template = %v, want nil", err)
	}
}

type recordingCreateActorClient struct {
	ateapi.ControlClient
	got *ateapi.CreateActorRequest
	err error
}

func (r *recordingCreateActorClient) CreateActor(_ context.Context, in *ateapi.CreateActorRequest, _ ...grpc.CallOption) (*ateapi.Actor, error) {
	r.got = in
	if r.err != nil {
		return nil, r.err
	}
	return in.GetActor(), nil
}

func TestCreateActor(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "claude-multiplex-demo", Name: "luna"}
	tmplRef := resources.ActorTemplateRef{Atespace: "claude-multiplex-demo", Name: "agent-luna"}

	t.Run("creates actor with template ref", func(t *testing.T) {
		rec := &recordingCreateActorClient{}
		client := &ateclient.Client{ControlClient: rec}
		if err := CreateActor(context.Background(), client, actorRef, tmplRef); err != nil {
			t.Fatalf("CreateActor = %v, want nil", err)
		}
		if got := resources.ActorRefFromActor(rec.got.GetActor()); got != actorRef {
			t.Errorf("created actor ref = %s, want %s", got, actorRef)
		}
		if got := resources.ActorTemplateRefFromObjectRef(rec.got.GetActor().GetActorTemplate()); got != tmplRef {
			t.Errorf("created actor template ref = %s, want %s", got, tmplRef)
		}
	})

	t.Run("idempotent on AlreadyExists", func(t *testing.T) {
		rec := &recordingCreateActorClient{err: status.Error(codes.AlreadyExists, "exists")}
		client := &ateclient.Client{ControlClient: rec}
		if err := CreateActor(context.Background(), client, actorRef, tmplRef); err != nil {
			t.Fatalf("CreateActor on AlreadyExists = %v, want nil", err)
		}
	})
}

type recordingCreateEgressPolicyClient struct {
	ateapi.ControlClient
	got *ateapi.CreateActorEgressPolicyRequest
	err error
}

func (r *recordingCreateEgressPolicyClient) CreateActorEgressPolicy(_ context.Context, in *ateapi.CreateActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapi.EgressPolicy, error) {
	r.got = in
	if r.err != nil {
		return nil, r.err
	}
	return in.GetEgressPolicy(), nil
}

func TestCreateActorEgressPolicy(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "claude-multiplex-demo", Name: "luna"}
	rules := []*ateapi.EgressRule{{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"api.anthropic.com"}}}}

	t.Run("creates the actor's singleton policy", func(t *testing.T) {
		rec := &recordingCreateEgressPolicyClient{}
		client := &ateclient.Client{ControlClient: rec}
		if err := CreateActorEgressPolicy(context.Background(), client, actorRef, rules); err != nil {
			t.Fatalf("CreateActorEgressPolicy = %v, want nil", err)
		}
		if got := resources.ActorRefFromObjectRef(rec.got.GetActor()); got != actorRef {
			t.Errorf("policy parent = %s, want %s", got, actorRef)
		}
		meta := rec.got.GetEgressPolicy().GetMetadata()
		if meta.GetAtespace() != actorRef.Atespace || meta.GetName() != resources.SingletonName {
			t.Errorf("policy metadata = %s/%s, want %s/%s", meta.GetAtespace(), meta.GetName(), actorRef.Atespace, resources.SingletonName)
		}
		if got := rec.got.GetEgressPolicy().GetRules(); len(got) != 1 || got[0] != rules[0] {
			t.Errorf("policy rules = %v, want %v", got, rules)
		}
	})

	t.Run("idempotent on AlreadyExists", func(t *testing.T) {
		rec := &recordingCreateEgressPolicyClient{err: status.Error(codes.AlreadyExists, "exists")}
		client := &ateclient.Client{ControlClient: rec}
		if err := CreateActorEgressPolicy(context.Background(), client, actorRef, rules); err != nil {
			t.Fatalf("CreateActorEgressPolicy on AlreadyExists = %v, want nil", err)
		}
	})
}

func TestExternalVolumeClassFromManifest(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot() = %v", err)
	}

	t.Run("checked-in manifests", func(t *testing.T) {
		nfsRaw, err := os.ReadFile(filepath.Join(root, "hack/csi/external-volume-class-nfs.yaml"))
		if err != nil {
			t.Fatalf("read nfs manifest: %v", err)
		}
		nfsClass, err := ExternalVolumeClassFromManifest(nfsRaw)
		if err != nil {
			t.Fatalf("ExternalVolumeClassFromManifest(nfs) = %v", err)
		}
		if nfsClass.GetMetadata().GetName() != "csi-nfs" {
			t.Errorf("nfs metadata.name = %q, want %q", nfsClass.GetMetadata().GetName(), "csi-nfs")
		}
		if nfsClass.GetDriverName() != "nfs.csi.k8s.io" {
			t.Errorf("nfs driverName = %q, want %q", nfsClass.GetDriverName(), "nfs.csi.k8s.io")
		}
		wantParams := map[string]string{
			"server": "nfs-server.default.svc.cluster.local",
			"share":  "/",
		}
		if got := nfsClass.GetParameters(); len(got) != len(wantParams) || got["server"] != wantParams["server"] || got["share"] != wantParams["share"] {
			t.Errorf("nfs parameters = %v, want %v", got, wantParams)
		}
		if got := nfsClass.GetMountOptions(); len(got) != 1 || got[0] != "nfsvers=4.1" {
			t.Errorf("nfs mountOptions = %v, want [nfsvers=4.1]", got)
		}

		hpRaw, err := os.ReadFile(filepath.Join(root, "hack/csi/external-volume-class-hostpath.yaml"))
		if err != nil {
			t.Fatalf("read hostpath manifest: %v", err)
		}
		hpClass, err := ExternalVolumeClassFromManifest(hpRaw)
		if err != nil {
			t.Fatalf("ExternalVolumeClassFromManifest(hostpath) = %v", err)
		}
		if hpClass.GetMetadata().GetName() != "csi-hostpath" {
			t.Errorf("hostpath metadata.name = %q, want %q", hpClass.GetMetadata().GetName(), "csi-hostpath")
		}
		if hpClass.GetDriverName() != "hostpath.csi.k8s.io" {
			t.Errorf("hostpath driverName = %q, want %q", hpClass.GetDriverName(), "hostpath.csi.k8s.io")
		}
		if len(hpClass.GetParameters()) != 0 {
			t.Errorf("hostpath parameters = %v, want empty", hpClass.GetParameters())
		}
		if len(hpClass.GetMountOptions()) != 0 {
			t.Errorf("hostpath mountOptions = %v, want empty", hpClass.GetMountOptions())
		}
	})

	errCases := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "invalid yaml", raw: ":\n  - [unclosed"},
		{name: "unknown field", raw: "metadata:\n  name: csi-nfs\nunknownField: value\n"},
	}
	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ExternalVolumeClassFromManifest([]byte(tc.raw)); err == nil {
				t.Fatalf("ExternalVolumeClassFromManifest(%s) = nil, want error", tc.name)
			}
		})
	}
}

func TestExternalVolumeClassesForDriver(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot() = %v", err)
	}
	cfg := &config.Config{Root: root}

	tests := []struct {
		driver string
		want   []string
	}{
		{driver: "none", want: nil},
		{driver: "nfs", want: []string{"csi-nfs"}},
		{driver: "hostpath", want: []string{"csi-hostpath"}},
		{driver: "both", want: []string{"csi-hostpath", "csi-nfs"}},
	}
	for _, tc := range tests {
		t.Run(tc.driver, func(t *testing.T) {
			if err := ValidateCSIDriver(tc.driver); err != nil {
				t.Fatalf("ValidateCSIDriver(%q) = %v", tc.driver, err)
			}
			spec := csiDrivers[tc.driver]
			classes, err := externalVolumeClassesForDriver(cfg, spec)
			if err != nil {
				t.Fatalf("externalVolumeClassesForDriver(%q) = %v", tc.driver, err)
			}
			if len(classes) != len(tc.want) {
				t.Fatalf("len(classes) = %d, want %d", len(classes), len(tc.want))
			}
			for i, name := range tc.want {
				if classes[i].GetMetadata().GetName() != name {
					t.Errorf("classes[%d].name = %q, want %q", i, classes[i].GetMetadata().GetName(), name)
				}
			}
		})
	}

	t.Run("missing manifest returns error", func(t *testing.T) {
		if err := ValidateCSIDriver("nfs"); err != nil {
			t.Fatalf("ValidateCSIDriver(nfs) = %v", err)
		}
		if _, err := externalVolumeClassesForDriver(&config.Config{Root: t.TempDir()}, csiDrivers["nfs"]); err == nil {
			t.Fatal("externalVolumeClassesForDriver with empty root = nil, want error")
		}
	})
}

func TestRegisterExternalVolumeClassesEarlySkipsAndErrors(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot() = %v", err)
	}

	t.Run("unknown driver returns error", func(t *testing.T) {
		env := &Env{Cfg: &config.Config{Root: root}}
		if err := env.RegisterExternalVolumeClasses(context.Background(), "bogus"); err == nil {
			t.Fatal("RegisterExternalVolumeClasses(bogus) = nil, want error")
		}
	})

	t.Run("driver none skips with gate enabled", func(t *testing.T) {
		env := &Env{Cfg: &config.Config{Root: root, PreviewGates: []string{"ExternalVolumes"}}}
		if err := env.RegisterExternalVolumeClasses(context.Background(), "none"); err != nil {
			t.Fatalf("RegisterExternalVolumeClasses(none) = %v, want nil", err)
		}
	})

	gateOffTests := []struct {
		name   string
		driver string
		gates  []string
	}{
		{name: "nfs empty gates", driver: "nfs", gates: nil},
		{name: "hostpath empty gates", driver: "hostpath", gates: nil},
		{name: "both unrelated gate", driver: "both", gates: []string{"OtherGate"}},
	}
	for _, tc := range gateOffTests {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{Cfg: &config.Config{Root: root, PreviewGates: tc.gates}}
			if err := env.RegisterExternalVolumeClasses(context.Background(), tc.driver); err != nil {
				t.Fatalf("RegisterExternalVolumeClasses(%q) with gate off = %v, want nil", tc.driver, err)
			}
		})
	}

	missingDeploymentTests := []struct {
		name   string
		driver string
		gates  []string
	}{
		{name: "ExternalVolumes gate", driver: "nfs", gates: []string{"ExternalVolumes"}},
		{name: "ALL gate", driver: "both", gates: []string{"ALL"}},
	}
	for _, tc := range missingDeploymentTests {
		t.Run("skips without ate-api-server "+tc.name, func(t *testing.T) {
			env := &Env{
				Cfg:  &config.Config{Root: root, PreviewGates: tc.gates},
				Kube: fakeKube(t),
			}
			if err := env.RegisterExternalVolumeClasses(context.Background(), tc.driver); err != nil {
				t.Fatalf("RegisterExternalVolumeClasses(%q) = %v, want nil", tc.driver, err)
			}
		})
	}
}

type fakeExternalVolumeClassClient struct {
	stored    map[string]*ateapi.ExternalVolumeClass
	createErr error
	getErr    error
	creates   int
	gets      int
}

func (f *fakeExternalVolumeClassClient) CreateExternalVolumeClass(_ context.Context, in *ateapi.CreateExternalVolumeClassRequest, _ ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error) {
	f.creates++
	if f.createErr != nil {
		return nil, f.createErr
	}
	evc := in.GetExternalVolumeClass()
	name := evc.GetMetadata().GetName()
	if _, ok := f.stored[name]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "external volume class %q already exists", name)
	}
	f.stored[name] = evc
	return evc, nil
}

func (f *fakeExternalVolumeClassClient) GetExternalVolumeClass(_ context.Context, in *ateapi.GetExternalVolumeClassRequest, _ ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error) {
	f.gets++
	if f.getErr != nil {
		return nil, f.getErr
	}
	name := in.GetExternalVolumeClass().GetName()
	evc, ok := f.stored[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "external volume class %q not found", name)
	}
	return evc, nil
}

func TestRegisterExternalVolumeClassesClient(t *testing.T) {
	nfs := &ateapi.ExternalVolumeClass{
		Metadata:     &ateapi.ResourceMetadata{Name: "csi-nfs"},
		DriverName:   "nfs.csi.k8s.io",
		Parameters:   map[string]string{"server": "nfs-server.default.svc.cluster.local", "share": "/"},
		MountOptions: []string{"nfsvers=4.1"},
	}
	hp := &ateapi.ExternalVolumeClass{
		Metadata:   &ateapi.ResourceMetadata{Name: "csi-hostpath"},
		DriverName: "hostpath.csi.k8s.io",
	}

	t.Run("creates missing classes", func(t *testing.T) {
		client := &fakeExternalVolumeClassClient{stored: map[string]*ateapi.ExternalVolumeClass{}}
		if err := registerExternalVolumeClasses(context.Background(), client, []*ateapi.ExternalVolumeClass{nfs, hp}); err != nil {
			t.Fatalf("registerExternalVolumeClasses = %v, want nil", err)
		}
		if client.creates != 2 || client.gets != 0 {
			t.Errorf("creates=%d gets=%d, want creates=2 gets=0", client.creates, client.gets)
		}
		if len(client.stored) != 2 {
			t.Errorf("stored %d classes, want 2", len(client.stored))
		}
	})

	t.Run("idempotent on AlreadyExists with matching spec", func(t *testing.T) {
		client := &fakeExternalVolumeClassClient{
			stored: map[string]*ateapi.ExternalVolumeClass{
				"csi-nfs": {
					Metadata:     &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "uid-1", Version: 3},
					DriverName:   "nfs.csi.k8s.io",
					Parameters:   map[string]string{"server": "nfs-server.default.svc.cluster.local", "share": "/"},
					MountOptions: []string{"nfsvers=4.1"},
				},
				"csi-hostpath": {
					Metadata:     &ateapi.ResourceMetadata{Name: "csi-hostpath", Uid: "uid-2", Version: 1},
					DriverName:   "hostpath.csi.k8s.io",
					Parameters:   map[string]string{},
					MountOptions: []string{},
				},
			},
		}
		if err := registerExternalVolumeClasses(context.Background(), client, []*ateapi.ExternalVolumeClass{nfs, hp}); err != nil {
			t.Fatalf("registerExternalVolumeClasses = %v, want nil", err)
		}
		if client.gets != 2 {
			t.Errorf("gets = %d, want 2", client.gets)
		}
	})

	mismatchCases := []struct {
		name     string
		existing *ateapi.ExternalVolumeClass
	}{
		{
			name: "driver mismatch",
			existing: &ateapi.ExternalVolumeClass{
				Metadata:     &ateapi.ResourceMetadata{Name: "csi-nfs"},
				DriverName:   "other.csi.k8s.io",
				Parameters:   map[string]string{"server": "nfs-server.default.svc.cluster.local", "share": "/"},
				MountOptions: []string{"nfsvers=4.1"},
			},
		},
		{
			name: "parameters mismatch",
			existing: &ateapi.ExternalVolumeClass{
				Metadata:     &ateapi.ResourceMetadata{Name: "csi-nfs"},
				DriverName:   "nfs.csi.k8s.io",
				Parameters:   map[string]string{"server": "other-nfs", "share": "/"},
				MountOptions: []string{"nfsvers=4.1"},
			},
		},
		{
			name: "mount options mismatch",
			existing: &ateapi.ExternalVolumeClass{
				Metadata:     &ateapi.ResourceMetadata{Name: "csi-nfs"},
				DriverName:   "nfs.csi.k8s.io",
				Parameters:   map[string]string{"server": "nfs-server.default.svc.cluster.local", "share": "/"},
				MountOptions: []string{"nfsvers=4.2"},
			},
		},
	}
	for _, tc := range mismatchCases {
		t.Run("AlreadyExists "+tc.name, func(t *testing.T) {
			client := &fakeExternalVolumeClassClient{
				stored: map[string]*ateapi.ExternalVolumeClass{"csi-nfs": tc.existing},
			}
			if err := registerExternalVolumeClasses(context.Background(), client, []*ateapi.ExternalVolumeClass{nfs}); err == nil {
				t.Fatalf("registerExternalVolumeClasses(%s) = nil, want error", tc.name)
			}
		})
	}

	t.Run("create error propagated", func(t *testing.T) {
		client := &fakeExternalVolumeClassClient{
			stored:    map[string]*ateapi.ExternalVolumeClass{},
			createErr: status.Error(codes.Unavailable, "unavailable"),
		}
		if err := registerExternalVolumeClasses(context.Background(), client, []*ateapi.ExternalVolumeClass{nfs}); err == nil {
			t.Fatal("registerExternalVolumeClasses on create error = nil, want error")
		}
	})

	t.Run("get error after AlreadyExists propagated", func(t *testing.T) {
		client := &fakeExternalVolumeClassClient{
			stored: map[string]*ateapi.ExternalVolumeClass{"csi-nfs": nfs},
			getErr: status.Error(codes.Internal, "internal"),
		}
		if err := registerExternalVolumeClasses(context.Background(), client, []*ateapi.ExternalVolumeClass{nfs}); err == nil {
			t.Fatal("registerExternalVolumeClasses on get error = nil, want error")
		}
	})
}
