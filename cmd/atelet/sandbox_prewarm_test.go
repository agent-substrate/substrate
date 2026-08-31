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

package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
)

func gvisorConfig(name, url, sha string) *v1alpha1.SandboxConfig {
	return &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.SandboxConfigSpec{
			SandboxClass: v1alpha1.SandboxClassGvisor,
			PauseImage:   "registry.k8s.io/pause@sha256:abc",
			Assets: map[string]map[string]v1alpha1.AssetFile{
				runtime.GOARCH: {
					runscAssetName: {URL: url, SHA256: sha},
				},
			},
		},
	}
}

func TestRecordFromSandboxConfig(t *testing.T) {
	sha := fmt.Sprintf("%x", sha256.Sum256([]byte("runsc")))
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", sha)

	rec, err := recordFromSandboxConfig(cfg)
	if err != nil {
		t.Fatalf("recordFromSandboxConfig: %v", err)
	}
	if rec.SandboxClass != string(v1alpha1.SandboxClassGvisor) {
		t.Errorf("SandboxClass = %q, want %q", rec.SandboxClass, v1alpha1.SandboxClassGvisor)
	}
	if rec.PauseImage != cfg.Spec.PauseImage {
		t.Errorf("PauseImage = %q, want %q", rec.PauseImage, cfg.Spec.PauseImage)
	}
	want := assetEntry{URL: "gs://bucket/runsc", SHA256: sha}
	if got := rec.Assets[runscAssetName]; got != want {
		t.Errorf("Assets[%q] = %+v, want %+v", runscAssetName, got, want)
	}

	// A config with no assets for this node's architecture cannot be projected.
	cfg.Spec.Assets = map[string]map[string]v1alpha1.AssetFile{
		"other-arch": {runscAssetName: {URL: "gs://bucket/runsc", SHA256: sha}},
	}
	if _, err := recordFromSandboxConfig(cfg); err == nil {
		t.Error("recordFromSandboxConfig accepted a config with no assets for the local architecture")
	}
}

func TestPrewarmEnqueueFilters(t *testing.T) {
	ctx := context.Background()
	microvm := &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "microvm-default"},
		Spec:       v1alpha1.SandboxConfigSpec{SandboxClass: v1alpha1.SandboxClassMicroVM},
	}
	gvisor := gvisorConfig("gvisor-default", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256([]byte("runsc"))))

	t.Run("node without KVM", func(t *testing.T) {
		p := &sandboxPrewarmer{queue: make(chan *v1alpha1.SandboxConfig, 1)}

		p.enqueue(ctx, "not a sandbox config")
		p.enqueue(ctx, microvm)
		p.enqueue(ctx, &v1alpha1.SandboxConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "future-class"},
			Spec:       v1alpha1.SandboxConfigSpec{SandboxClass: "future-class"},
		})
		if len(p.queue) != 0 {
			t.Fatalf("queue holds %d configs after filtered enqueues, want 0", len(p.queue))
		}

		p.enqueue(ctx, gvisor)
		if len(p.queue) != 1 {
			t.Fatalf("queue holds %d configs after gvisor enqueue, want 1", len(p.queue))
		}
		// A full queue must drop rather than block the informer handler.
		p.enqueue(ctx, gvisor)
		if len(p.queue) != 1 {
			t.Errorf("queue holds %d configs after enqueue on a full queue, want 1", len(p.queue))
		}
	})

	t.Run("node with KVM", func(t *testing.T) {
		p := &sandboxPrewarmer{queue: make(chan *v1alpha1.SandboxConfig, 2), microvmCapable: true}
		p.enqueue(ctx, microvm)
		p.enqueue(ctx, gvisor)
		if len(p.queue) != 2 {
			t.Errorf("queue holds %d configs, want both microvm and gvisor queued", len(p.queue))
		}
	})
}

// TestMicrovmNodeCapable covers the detectable negative cases; the positive
// case needs a /dev/kvm character device, which a test cannot mknod.
func TestMicrovmNodeCapable(t *testing.T) {
	devRoot := t.TempDir()
	if microvmNodeCapable(devRoot) {
		t.Error("microvmNodeCapable = true for a dev root without kvm")
	}
	// A plain file named kvm is not a character device and must not count.
	if err := os.WriteFile(devRoot+"/kvm", []byte("not a device"), 0o600); err != nil {
		t.Fatal(err)
	}
	if microvmNodeCapable(devRoot) {
		t.Error("microvmNodeCapable = true for a regular file named kvm")
	}
}

// TestSandboxAssetPrewarmDownloads runs the whole path: a SandboxConfig in a
// fake clientset flows through the informer into the prewarm worker, which
// lands the asset in the static-files cache without any Run/Restore request.
func TestSandboxAssetPrewarmDownloads(t *testing.T) {
	origDir, origJitter := ateompath.StaticFilesDir, prewarmMaxJitter
	ateompath.StaticFilesDir = t.TempDir()
	prewarmMaxJitter = 0
	t.Cleanup(func() { ateompath.StaticFilesDir, prewarmMaxJitter = origDir, origJitter })

	content := []byte("runsc binary bytes")
	sha := fmt.Sprintf("%x", sha256.Sum256(content))
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", sha)

	ctx := t.Context()
	client := fake.NewSimpleClientset(cfg)
	factory := externalversions.NewSharedInformerFactory(client, 0)
	informer := factory.Api().V1alpha1().SandboxConfigs().Informer()
	stopCh := make(chan struct{})
	defer close(stopCh)
	factory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
		t.Fatal("informer cache never synced")
	}

	herder := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
	if err := startSandboxAssetPrewarm(ctx, informer, herder, false); err != nil {
		t.Fatalf("startSandboxAssetPrewarm: %v", err)
	}

	wantPath := ateompath.RunSCBinaryPath(sha)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(wantPath); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %s: %v", wantPath, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("asset never prewarmed to %s", wantPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
