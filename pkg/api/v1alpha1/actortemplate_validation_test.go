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

package v1alpha1

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/envtestbins"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

func TestMain(m *testing.M) {
	binaryAssetsDirectory, err := envtestbins.BinaryAssetsDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../../manifests/ate-install/generated"},
		BinaryAssetsDirectory: binaryAssetsDirectory,
	}

	cfg, err = testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest start failed: %v\n", err)
		testEnv.Stop()
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(AddToScheme(scheme))

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "k8s client creation failed: %v\n", err)
		testEnv.Stop()
		os.Exit(1)
	}

	code := m.Run()

	_ = testEnv.Stop()
	os.Exit(code)
}

func TestActorTemplateValidation(t *testing.T) {
	ctx := t.Context()

	baseTemplate := &ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
		Spec: ActorTemplateSpec{
			PauseImage: "gcr.io/gke-release/pause@sha256:bcbd57ba5653580ec647b16d8163cdd1112df3609129b01f912a8032e48265da",
			Containers: []Container{
				{
					Name:  "main",
					Image: "busybox@sha256:326e0e090a9a4057e62a1b94236e7a2df2f2f76722f67232e0e47854e4df9c53",
				},
			},
			SnapshotsConfig: SnapshotsConfig{
				Location: "gs://test-bucket/test-folder",
			},
			WorkerPoolRef: corev1.ObjectReference{
				Name: "test-pool",
			},
			Runsc: RunscConfig{
				AMD64: &RunscPlatformConfig{
					URL:        "gs://bucket/runsc",
					SHA256Hash: "deadbeef",
				},
			},
		},
	}

	tests := []struct {
		name    string
		mutate  func(*ActorTemplate)
		wantErr bool
		errMsg  string
	}{{
		name:    "base template",
		mutate:  func(at *ActorTemplate) {},
		wantErr: false,
	}, {
		name: "missing PauseImage",
		mutate: func(at *ActorTemplate) {
			at.Spec.PauseImage = ""
		},
		wantErr: true,
		errMsg:  "Required value",
	}, {
		name: "unpinned PauseImage",
		mutate: func(at *ActorTemplate) {
			at.Spec.PauseImage = "pause"
		},
		wantErr: true,
		errMsg:  "All images must be pinned",
	}, {
		name: "missing SnapshotsConfig.Location",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.Location = ""
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}, {
		name: "missing Runsc.AMD64.URL",
		mutate: func(at *ActorTemplate) {
			at.Spec.Runsc.AMD64.URL = ""
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}, {
		name: "missing Runsc.AMD64.SHA256Hash",
		mutate: func(at *ActorTemplate) {
			at.Spec.Runsc.AMD64.SHA256Hash = ""
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}, {
		name: "too many containers",
		mutate: func(at *ActorTemplate) {
			for i := 1; i <= 10; i++ {
				at.Spec.Containers = append(at.Spec.Containers, at.Spec.Containers[0])
				at.Spec.Containers[i].Name = fmt.Sprintf("container-%d", i)
			}
		},
		wantErr: true,
		errMsg:  "Too many",
	}, {
		name: "empty container name",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Name = ""
		},
		wantErr: true,
		errMsg:  "must be a valid DNS label",
	}, {
		name: "too-long container name",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Name = strings.Repeat("x", 64)
		},
		wantErr: true,
		errMsg:  "Too long",
	}, {
		name: "invalid container name",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Name = "Invalid Name"
		},
		wantErr: true,
		errMsg:  "must be a valid DNS label",
	}, {
		name: "empty container Image",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Image = ""
		},
		wantErr: true,
		errMsg:  "Required value",
	}, {
		name: "unpinned container Image",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Image = "busybox"
		},
		wantErr: true,
		errMsg:  "All images must be pinned",
	}, {
		name: "valid container Command",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Command = []string{"command"}
		},
		wantErr: false,
	}, {
		name: "long container Command",
		mutate: func(at *ActorTemplate) {
			for range 64 {
				at.Spec.Containers[0].Command = append(at.Spec.Containers[0].Command, "x")
			}
		},
		wantErr: false,
	}, {
		name: "too-many container Command",
		mutate: func(at *ActorTemplate) {
			for range 65 {
				at.Spec.Containers[0].Command = append(at.Spec.Containers[0].Command, "x")
			}
		},
		wantErr: true,
		errMsg:  "Too many",
	}, {
		name: "valid EnvVar",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{
				{Name: "FOO", Value: ptr.To("BAR")},
			}
		},
		wantErr: false,
	}, {
		name: "long EnvVar",
		mutate: func(at *ActorTemplate) {
			for range 32 {
				at.Spec.Containers[0].Env = append(at.Spec.Containers[0].Env, EnvVar{Name: "X", Value: ptr.To("Y")})
			}
		},
		wantErr: false,
	}, {
		name: "too-many EnvVar",
		mutate: func(at *ActorTemplate) {
			for range 33 {
				at.Spec.Containers[0].Env = append(at.Spec.Containers[0].Env, EnvVar{Name: "X", Value: ptr.To("Y")})
			}
		},
		wantErr: true,
		errMsg:  "Too many",
	}, {
		name: "envVar Name with space",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{Name: "FOO BAR", Value: ptr.To("VAL")}}
		},
		wantErr: false, // strange but valid
	}, {
		name: "empty EnvVar Name",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{Name: "", Value: ptr.To("VAL")}}
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}, {
		name: "invalid EnvVar Name (contains '=')",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{Name: "FOO=BAR", Value: ptr.To("VAL")}}
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}, {
		name: "missing EnvVar Value",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{Name: "FOO"}}
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}, {
		name: "EnvVar with ValueFrom SecretKeyRef",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name: "FOO",
				ValueFrom: &EnvVarSource{
					SecretKeyRef: &SecretKeySelector{
						Name: "my-secret",
						Key:  "my-key",
					},
				},
			}}
		},
		wantErr: false,
	}, {
		name: "EnvVar with both Value and ValueFrom",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name:  "FOO",
				Value: ptr.To("BAR"),
				ValueFrom: &EnvVarSource{
					SecretKeyRef: &SecretKeySelector{
						Name: "my-secret",
						Key:  "my-key",
					},
				},
			}}
		},
		wantErr: true,
		errMsg:  "exactly one of the fields in",
	}, {
		name: "EnvVarSource empty",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name:      "FOO",
				ValueFrom: &EnvVarSource{},
			}}
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}, {
		name: "SecretKeySelector missing Name",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name: "FOO",
				ValueFrom: &EnvVarSource{
					SecretKeyRef: &SecretKeySelector{
						Key: "my-key",
					},
				},
			}}
		},
		wantErr: true,
		errMsg:  "Name must be a valid DNS subdomain",
	}, {
		name: "SecretKeySelector Name too long",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name: "FOO",
				ValueFrom: &EnvVarSource{
					SecretKeyRef: &SecretKeySelector{
						Name: strings.Repeat("x", 254),
						Key:  "my-key",
					},
				},
			}}
		},
		wantErr: true,
		errMsg:  "Too long",
	}, {
		name: "SecretKeySelector invalid Name",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name: "FOO",
				ValueFrom: &EnvVarSource{
					SecretKeyRef: &SecretKeySelector{
						Name: "Invalid_Name",
						Key:  "my-key",
					},
				},
			}}
		},
		wantErr: true,
		errMsg:  "Name must be a valid DNS subdomain",
	}, {
		name: "SecretKeySelector missing Key",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name: "FOO",
				ValueFrom: &EnvVarSource{
					SecretKeyRef: &SecretKeySelector{
						Name: "my-secret",
					},
				},
			}}
		},
		wantErr: true,
		errMsg:  "at least 1 chars long",
	}, {
		name: "SecretKeySelector invalid Key",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Env = []EnvVar{{
				Name: "FOO",
				ValueFrom: &EnvVarSource{
					SecretKeyRef: &SecretKeySelector{
						Name: "my-secret",
						Key:  "invalid/key",
					},
				},
			}}
		},
		wantErr: true,
		errMsg:  "Invalid value",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := baseTemplate.DeepCopy()
			tt.mutate(at)

			err := k8sClient.Create(ctx, at)
			if err != nil && !tt.wantErr {
				t.Errorf("unexpected failure: %v", err)
			}
			if err == nil && tt.wantErr {
				t.Errorf("unexpected success, expected %q", tt.errMsg)
			}
			if err != nil && tt.wantErr && tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("wrong error:\n  wanted: %q\n     got: %q", tt.errMsg, err.Error())
			}
			if err == nil {
				_ = k8sClient.Delete(ctx, at)
			}
		})
	}
}

func TestActorTemplateSpecImmutability(t *testing.T) {
	ctx := t.Context()

	baseTemplate := &ActorTemplate{
		Spec: ActorTemplateSpec{
			PauseImage: "pause@hash",
			Containers: []Container{
				{
					Name:  "main",
					Image: "busybox@hash",
				},
			},
			SnapshotsConfig: SnapshotsConfig{
				Location: "gs://test-bucket/test-folder",
			},
			WorkerPoolRef: corev1.ObjectReference{
				Name: "test-pool",
			},
			Runsc: RunscConfig{
				AMD64: &RunscPlatformConfig{
					URL:        "gs://bucket/runsc",
					SHA256Hash: "deadbeef",
				},
			},
		},
	}

	tests := []struct {
		name   string
		mutate func(*ActorTemplate)
	}{
		{
			name: "update-pause-image",
			mutate: func(at *ActorTemplate) {
				at.Spec.PauseImage = "pause@new"
			},
		},
		{
			name: "update-snapshots-config-location",
			mutate: func(at *ActorTemplate) {
				at.Spec.SnapshotsConfig.Location = "gs://new-bucket/new-folder"
			},
		},
		{
			name: "update-worker-pool-ref-name",
			mutate: func(at *ActorTemplate) {
				at.Spec.WorkerPoolRef.Name = "new-pool"
			},
		},
		{
			name: "update-runsc-amd64-url",
			mutate: func(at *ActorTemplate) {
				at.Spec.Runsc.AMD64.URL = "gs://new-bucket/runsc"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := namespaceForTest(tt.name)
			namespaceObj := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}
			if err := k8sClient.Create(ctx, namespaceObj); err != nil {
				t.Fatalf("failed to create namespace: %v", err)
			}
			defer func() {
				_ = k8sClient.Delete(ctx, namespaceObj)
			}()

			at := baseTemplate.DeepCopy()
			at.Namespace = ns
			at.Name = "test"

			if err := k8sClient.Create(ctx, at); err != nil {
				t.Fatalf("failed to create ActorTemplate: %v", err)
			}
			defer func() {
				_ = k8sClient.Delete(ctx, at)
			}()

			updatedAt := at.DeepCopy()
			tt.mutate(updatedAt)

			err := k8sClient.Update(ctx, updatedAt)
			if err == nil {
				t.Error("expected update to fail due to immutability, but it succeeded")
			} else if !strings.Contains(err.Error(), "Spec is immutable") {
				t.Errorf("expected error containing 'Spec is immutable', got: %v", err)
			}
		})
	}
}

func namespaceForTest(testName string) string {
	return fmt.Sprintf("%s-%d", testName, time.Now().UnixNano())
}
