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
	"strconv"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"

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

func TestCSIDriverConfigFromManifest(t *testing.T) {
	root := repoRoot(t)

	var nfsSvc, hpSvc corev1.Service
	decodeYAML(t, "csiNFSControllerService", csiNFSControllerService, &nfsSvc)
	decodeYAML(t, "csiHostpathControllerService", csiHostpathControllerService, &hpSvc)

	for _, tc := range []struct {
		relPath string
		name    string
		driver  string
		socket  string
		svc     corev1.Service
	}{
		{
			relPath: "hack/csi/csi-driver-config-nfs.yaml",
			name:    "csi-nfs",
			driver:  "nfs.csi.k8s.io",
			socket:  "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock",
			svc:     nfsSvc,
		},
		{
			relPath: "hack/csi/csi-driver-config-hostpath.yaml",
			name:    "csi-hostpath",
			driver:  "hostpath.csi.k8s.io",
			socket:  "unix:///var/lib/kubelet/plugins/csi-hostpath/csi.sock",
			svc:     hpSvc,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(root, tc.relPath))
			if err != nil {
				t.Fatalf("ReadFile(%s): %v", tc.relPath, err)
			}
			cfg, err := CSIDriverConfigFromManifest(raw)
			if err != nil {
				t.Fatalf("CSIDriverConfigFromManifest(%s): %v", tc.relPath, err)
			}
			if cfg.GetMetadata().GetName() != tc.name {
				t.Errorf("metadata.name = %q, want %q", cfg.GetMetadata().GetName(), tc.name)
			}
			if cfg.GetDriverName() != tc.driver {
				t.Errorf("driverName = %q, want %q", cfg.GetDriverName(), tc.driver)
			}
			if cfg.GetNodeSocketOverride() != tc.socket {
				t.Errorf("nodeSocketOverride = %q, want %q", cfg.GetNodeSocketOverride(), tc.socket)
			}
			wantEndpoint := "tcp://" + tc.svc.Name + "." + tc.svc.Namespace + ".svc.cluster.local:" + strconv.Itoa(int(tc.svc.Spec.Ports[0].Port))
			if cfg.GetControllerEndpoint() != wantEndpoint {
				t.Errorf("controllerEndpoint = %q, want %q", cfg.GetControllerEndpoint(), wantEndpoint)
			}
			wantServer := tc.svc.Name + "." + tc.svc.Namespace + ".svc"
			if !cfg.GetTls().GetEnabled() || !cfg.GetTls().GetUsePodIdentity() || cfg.GetTls().GetServerName() != wantServer {
				t.Errorf("tls = %+v, want enabled pod-identity with serverName %q", cfg.GetTls(), wantServer)
			}
		})
	}

	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "invalid yaml", raw: ":\n  - [unclosed"},
		{name: "unknown field", raw: "metadata:\n  name: csi-nfs\nunknownField: value\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := CSIDriverConfigFromManifest([]byte(tc.raw)); err == nil {
				t.Fatalf("CSIDriverConfigFromManifest(%s) = nil, want error", tc.name)
			}
		})
	}
}

func TestCSIDriverConfigsForDriver(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	for _, tc := range []struct {
		driver string
		want   []string
	}{
		{driver: "none", want: nil},
		{driver: "nfs", want: []string{"csi-nfs"}},
		{driver: "hostpath", want: []string{"csi-hostpath"}},
		{driver: "both", want: []string{"csi-hostpath", "csi-nfs"}},
	} {
		t.Run(tc.driver, func(t *testing.T) {
			configs, err := csiDriverConfigsForDriver(cfg, csiDrivers[tc.driver])
			if err != nil {
				t.Fatalf("csiDriverConfigsForDriver(%q) = %v", tc.driver, err)
			}
			if len(configs) != len(tc.want) {
				t.Fatalf("len(configs) = %d, want %d", len(configs), len(tc.want))
			}
			for i, name := range tc.want {
				if configs[i].GetMetadata().GetName() != name {
					t.Errorf("configs[%d].name = %q, want %q", i, configs[i].GetMetadata().GetName(), name)
				}
			}
		})
	}

	t.Run("missing manifest returns error", func(t *testing.T) {
		if _, err := csiDriverConfigsForDriver(&config.Config{Root: t.TempDir()}, csiDrivers["nfs"]); err == nil {
			t.Fatal("csiDriverConfigsForDriver with empty root = nil, want error")
		}
	})
}

func TestRegisterCSIDriverConfigsEarlySkipsAndErrors(t *testing.T) {
	root := repoRoot(t)

	t.Run("unknown driver returns error", func(t *testing.T) {
		env := &Env{Cfg: &config.Config{Root: root}}
		if err := env.RegisterCSIDriverConfigs(context.Background(), "bogus"); err == nil {
			t.Fatal("RegisterCSIDriverConfigs(bogus) = nil, want error")
		}
	})

	t.Run("driver none skips with gate enabled", func(t *testing.T) {
		env := &Env{Cfg: &config.Config{Root: root, PreviewGates: []string{"ExternalVolumes"}}}
		if err := env.RegisterCSIDriverConfigs(context.Background(), "none"); err != nil {
			t.Fatalf("RegisterCSIDriverConfigs(none) = %v, want nil", err)
		}
	})

	for _, tc := range []struct {
		name   string
		driver string
		gates  []string
	}{
		{name: "nfs empty gates", driver: "nfs"},
		{name: "hostpath empty gates", driver: "hostpath"},
		{name: "both unrelated gate", driver: "both", gates: []string{"OtherGate"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{Cfg: &config.Config{Root: root, PreviewGates: tc.gates}}
			if err := env.RegisterCSIDriverConfigs(context.Background(), tc.driver); err != nil {
				t.Fatalf("RegisterCSIDriverConfigs(%q) with gate off = %v, want nil", tc.driver, err)
			}
		})
	}

	for _, gates := range [][]string{{"ExternalVolumes"}, {"ALL"}} {
		t.Run("skips without ate-api-server "+gates[0], func(t *testing.T) {
			env := &Env{
				Cfg:  &config.Config{Root: root, PreviewGates: gates},
				Kube: fakeKube(t),
			}
			if err := env.RegisterCSIDriverConfigs(context.Background(), "both"); err != nil {
				t.Fatalf("RegisterCSIDriverConfigs(both) = %v, want nil", err)
			}
		})
	}
}

type fakeCSIDriverConfigClient struct {
	stored    map[string]*ateapi.CsiDriverConfig
	createErr error
	getErr    error
	updateErr error
	creates   int
	gets      int
	updates   int
	lastUpd   *ateapi.CsiDriverConfig
}

func (f *fakeCSIDriverConfigClient) CreateCsiDriverConfig(_ context.Context, in *ateapi.CreateCsiDriverConfigRequest, _ ...grpc.CallOption) (*ateapi.CsiDriverConfig, error) {
	f.creates++
	if f.createErr != nil {
		return nil, f.createErr
	}
	cfg := in.GetCsiDriverConfig()
	name := cfg.GetMetadata().GetName()
	for existingName, existing := range f.stored {
		if existingName == name || existing.GetDriverName() == cfg.GetDriverName() {
			return nil, status.Errorf(codes.AlreadyExists, "csi driver config %q already exists", name)
		}
	}
	f.stored[name] = proto.CloneOf(cfg)
	return cfg, nil
}

func (f *fakeCSIDriverConfigClient) GetCsiDriverConfig(_ context.Context, in *ateapi.GetCsiDriverConfigRequest, _ ...grpc.CallOption) (*ateapi.CsiDriverConfig, error) {
	f.gets++
	if f.getErr != nil {
		return nil, f.getErr
	}
	name := in.GetCsiDriverConfig().GetName()
	cfg, ok := f.stored[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "csi driver config %q not found", name)
	}
	return proto.CloneOf(cfg), nil
}

func (f *fakeCSIDriverConfigClient) UpdateCsiDriverConfig(_ context.Context, in *ateapi.UpdateCsiDriverConfigRequest, _ ...grpc.CallOption) (*ateapi.CsiDriverConfig, error) {
	f.updates++
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	cfg := proto.CloneOf(in.GetCsiDriverConfig())
	f.lastUpd = cfg
	f.stored[cfg.GetMetadata().GetName()] = cfg
	return cfg, nil
}

func TestRegisterCSIDriverConfigsClient(t *testing.T) {
	const storedUID = "11111111-2222-3333-4444-555555555555"
	nfs := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs"},
		DriverName:         "nfs.csi.k8s.io",
		ControllerEndpoint: "tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052",
		NodeSocketOverride: "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock",
		Tls: &ateapi.CsiDriverTlsConfig{
			Enabled:        true,
			UsePodIdentity: true,
			ServerName:     "csi-nfs-controller.kube-system.svc",
		},
	}
	plain := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-plain"},
		DriverName:         "plain.csi.k8s.io",
		ControllerEndpoint: "tcp://127.0.0.1:9000",
	}

	t.Run("creates missing configs", func(t *testing.T) {
		client := &fakeCSIDriverConfigClient{stored: map[string]*ateapi.CsiDriverConfig{}}
		if err := registerCSIDriverConfigs(context.Background(), client, []*ateapi.CsiDriverConfig{nfs, plain}); err != nil {
			t.Fatalf("registerCSIDriverConfigs = %v, want nil", err)
		}
		if client.creates != 2 || client.gets != 0 || client.updates != 0 {
			t.Errorf("creates=%d gets=%d updates=%d, want 2/0/0", client.creates, client.gets, client.updates)
		}
	})

	t.Run("AlreadyExists matching spec keeps existing without update", func(t *testing.T) {
		existingNFS := proto.CloneOf(nfs)
		existingNFS.Metadata = &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: storedUID, Version: 4}
		existingPlain := proto.CloneOf(plain)
		existingPlain.Metadata = &ateapi.ResourceMetadata{Name: "csi-plain", Uid: storedUID, Version: 2}
		existingPlain.Tls = &ateapi.CsiDriverTlsConfig{} // empty Tls matches nil Tls
		client := &fakeCSIDriverConfigClient{
			stored: map[string]*ateapi.CsiDriverConfig{"csi-nfs": existingNFS, "csi-plain": existingPlain},
		}
		if err := registerCSIDriverConfigs(context.Background(), client, []*ateapi.CsiDriverConfig{nfs, plain}); err != nil {
			t.Fatalf("registerCSIDriverConfigs = %v, want nil", err)
		}
		if client.gets != 2 || client.updates != 0 {
			t.Errorf("gets=%d updates=%d, want 2/0", client.gets, client.updates)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(*ateapi.CsiDriverConfig)
	}{
		{name: "controllerEndpoint drift", mutate: func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://10.0.0.9:50052" }},
		{name: "nodeSocketOverride drift", mutate: func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///tmp/old.sock" }},
		{name: "tls drift", mutate: func(c *ateapi.CsiDriverConfig) { c.Tls = nil }},
	} {
		t.Run("AlreadyExists "+tc.name+" updates with stored uid and version", func(t *testing.T) {
			existing := proto.CloneOf(nfs)
			existing.Metadata = &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: storedUID, Version: 7}
			tc.mutate(existing)
			client := &fakeCSIDriverConfigClient{stored: map[string]*ateapi.CsiDriverConfig{"csi-nfs": existing}}
			if err := registerCSIDriverConfigs(context.Background(), client, []*ateapi.CsiDriverConfig{nfs}); err != nil {
				t.Fatalf("registerCSIDriverConfigs(%s) = %v, want nil", tc.name, err)
			}
			if client.updates != 1 {
				t.Fatalf("updates = %d, want 1", client.updates)
			}
			if got := client.lastUpd.GetMetadata(); got.GetName() != "csi-nfs" || got.GetUid() != storedUID || got.GetVersion() != 7 {
				t.Errorf("update metadata = %+v, want name=csi-nfs uid=%s version=7", got, storedUID)
			}
			if !csiDriverConfigSpecEqual(client.lastUpd, nfs) {
				t.Errorf("updated spec = %+v, want %+v", client.lastUpd, nfs)
			}
			if nfs.GetMetadata().GetUid() != "" {
				t.Errorf("input config mutated: uid = %q", nfs.GetMetadata().GetUid())
			}
		})
	}

	t.Run("AlreadyExists immutable driverName mismatch fails", func(t *testing.T) {
		existing := proto.CloneOf(nfs)
		existing.DriverName = "other.csi.k8s.io"
		client := &fakeCSIDriverConfigClient{stored: map[string]*ateapi.CsiDriverConfig{"csi-nfs": existing}}
		err := registerCSIDriverConfigs(context.Background(), client, []*ateapi.CsiDriverConfig{nfs})
		if err == nil || !strings.Contains(err.Error(), "driverName") {
			t.Fatalf("registerCSIDriverConfigs = %v, want driverName mismatch error", err)
		}
		if client.updates != 0 {
			t.Errorf("updates = %d, want 0", client.updates)
		}
	})

	t.Run("AlreadyExists with Get NotFound reports conflicting driver registration", func(t *testing.T) {
		other := proto.CloneOf(nfs)
		other.Metadata = &ateapi.ResourceMetadata{Name: "custom-nfs"}
		client := &fakeCSIDriverConfigClient{stored: map[string]*ateapi.CsiDriverConfig{"custom-nfs": other}}
		err := registerCSIDriverConfigs(context.Background(), client, []*ateapi.CsiDriverConfig{nfs})
		if err == nil || !strings.Contains(err.Error(), "nfs.csi.k8s.io") {
			t.Fatalf("registerCSIDriverConfigs = %v, want error naming driver nfs.csi.k8s.io", err)
		}
	})

	t.Run("propagates RPC errors", func(t *testing.T) {
		if err := registerCSIDriverConfigs(context.Background(), &fakeCSIDriverConfigClient{
			stored:    map[string]*ateapi.CsiDriverConfig{},
			createErr: status.Error(codes.Unavailable, "unavailable"),
		}, []*ateapi.CsiDriverConfig{nfs}); err == nil {
			t.Error("create error = nil, want error")
		}
		if err := registerCSIDriverConfigs(context.Background(), &fakeCSIDriverConfigClient{
			stored: map[string]*ateapi.CsiDriverConfig{"csi-nfs": nfs},
			getErr: status.Error(codes.Internal, "internal"),
		}, []*ateapi.CsiDriverConfig{nfs}); err == nil {
			t.Error("get error = nil, want error")
		}
		drifted := proto.CloneOf(nfs)
		drifted.ControllerEndpoint = "tcp://10.0.0.9:50052"
		if err := registerCSIDriverConfigs(context.Background(), &fakeCSIDriverConfigClient{
			stored:    map[string]*ateapi.CsiDriverConfig{"csi-nfs": drifted},
			updateErr: status.Error(codes.Aborted, "conflict"),
		}, []*ateapi.CsiDriverConfig{nfs}); err == nil {
			t.Error("update error = nil, want error")
		}
	})
}
