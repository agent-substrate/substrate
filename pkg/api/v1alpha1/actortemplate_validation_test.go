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

	"github.com/agent-substrate/substrate/internal/testenv"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	cfg       *rest.Config
	k8sClient client.Client
)

func TestMain(m *testing.M) {
	var stopEnv func()
	cfg, stopEnv = testenv.Start()

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(AddToScheme(scheme))

	var err error
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "k8s client creation failed: %v\n", err)
		stopEnv()
		os.Exit(1)
	}

	code := m.Run()

	stopEnv()
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
			Containers: []Container{
				{
					Name:  "main",
					Image: "busybox@sha256:326e0e090a9a4057e62a1b94236e7a2df2f2f76722f67232e0e47854e4df9c53",
				},
			},
			SnapshotsConfig: SnapshotsConfig{
				Location: "gs://test-bucket/test-folder",
			},
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"pool": "test-pool"},
			},
		},
	}

	tests := []struct {
		name    string
		mutate  func(*ActorTemplate)
		wantErr bool
		errMsg  string
		// verify runs on the created object for cases that assert what the API
		// server stored rather than whether it accepted the create.
		verify func(*testing.T, *ActorTemplate)
	}{{
		name:    "base template",
		mutate:  func(at *ActorTemplate) {},
		wantErr: false,
	}, {
		name: "missing SnapshotsConfig.Location",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.Location = ""
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
		name: "valid container Args",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Args = []string{"arg"}
		},
		wantErr: false,
	}, {
		name: "long container Args",
		mutate: func(at *ActorTemplate) {
			for range 64 {
				at.Spec.Containers[0].Args = append(at.Spec.Containers[0].Args, "x")
			}
		},
		wantErr: false,
	}, {
		name: "too-many container Args",
		mutate: func(at *ActorTemplate) {
			for range 65 {
				at.Spec.Containers[0].Args = append(at.Spec.Containers[0].Args, "x")
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
	}, {
		name: "valid Readyz with default path",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Port: 8080},
			}
		},
		wantErr: false,
	}, {
		name: "valid Readyz with explicit path",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "/health", Port: 8080},
			}
		},
		wantErr: false,
	}, {
		name: "Readyz missing HTTPGet",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{}
		},
		wantErr: true,
		errMsg:  "Required value",
	}, {
		name: "Readyz port zero",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Port: 0},
			}
		},
		wantErr: true,
		errMsg:  "should be greater than or equal to 1",
	}, {
		name: "Readyz port too large",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Port: 65536},
			}
		},
		wantErr: true,
		errMsg:  "should be less than or equal to 65535",
	}, {
		name: "Readyz Path with nested segments and percent encoding",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "/v1/health/check%20me", Port: 80},
			}
		},
		wantErr: false,
	}, {
		name: "Readyz Path missing leading slash",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "readyz", Port: 80},
			}
		},
		wantErr: true,
		errMsg:  "should match",
	}, {
		name: "Readyz Path with query string",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "/readyz?check=1", Port: 80},
			}
		},
		wantErr: true,
		errMsg:  "should match",
	}, {
		name: "Readyz Path with fragment",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "/readyz#frag", Port: 80},
			}
		},
		wantErr: true,
		errMsg:  "should match",
	}, {
		name: "Readyz Path with whitespace",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "/ready z", Port: 80},
			}
		},
		wantErr: true,
		errMsg:  "should match",
	}, {
		name: "Readyz Path with bare percent",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "/foo%", Port: 80},
			}
		},
		wantErr: true,
		errMsg:  "should match",
	}, {
		name: "Readyz Path with malformed percent-escape",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Path: "/bar%zz", Port: 80},
			}
		},
		wantErr: true,
		errMsg:  "should match",
	}, {
		// A probe that declares only a port reads back with the omitted fields
		// filled in, so a template author can see the effective readiness
		// settings on the object rather than having to know what the ateom
		// would substitute.
		name: "Readyz omitted fields are defaulted by the API server",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet: &HTTPGetAction{Port: 80},
			}
		},
		wantErr: false,
		verify: func(t *testing.T, at *ActorTemplate) {
			readyz := at.Spec.Containers[0].Readyz
			if want, got := "/readyz", readyz.HTTPGet.Path; got != want {
				t.Errorf("Readyz.HTTPGet.Path = %q, want %q (CRD default)", got, want)
			}
			if want, got := int32(30), readyz.TimeoutSeconds; got != want {
				t.Errorf("Readyz.TimeoutSeconds = %d, want %d (CRD default)", got, want)
			}
		},
	}, {
		name: "valid Readyz TimeoutSeconds",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet:        &HTTPGetAction{Port: 80},
				TimeoutSeconds: 300,
			}
		},
		wantErr: false,
		verify: func(t *testing.T, at *ActorTemplate) {
			if want, got := int32(300), at.Spec.Containers[0].Readyz.TimeoutSeconds; got != want {
				t.Errorf("Readyz.TimeoutSeconds = %d, want %d (explicit value must survive defaulting)", got, want)
			}
		},
	}, {
		// A zero deadline could never be met. The field omits its zero value,
		// so 0 from a Go client is indistinguishable from unset and defaults to
		// 30; a manifest that spells out 0 is rejected by the same bound this
		// case exercises.
		name: "Readyz TimeoutSeconds below the minimum",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet:        &HTTPGetAction{Port: 80},
				TimeoutSeconds: -1,
			}
		},
		wantErr: true,
		errMsg:  "should be greater than or equal to 1",
	}, {
		name: "Readyz TimeoutSeconds above the maximum",
		mutate: func(at *ActorTemplate) {
			at.Spec.Containers[0].Readyz = &ContainerReadyz{
				HTTPGet:        &HTTPGetAction{Port: 80},
				TimeoutSeconds: 3601,
			}
		},
		wantErr: true,
		errMsg:  "should be less than or equal to 3600",
	}, {
		name: "valid SandboxClass microvm",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
		},
		wantErr: false,
	}, {
		name: "invalid SandboxClass",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = "kvm"
		},
		wantErr: true,
		errMsg:  "Unsupported value",
	}, {
		name: "SnapshotsConfig: OnPause=Full, OnCommit=Full",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnPause = SnapshotScopeFull
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeFull
		},
		wantErr: false,
	}, {
		name: "SnapshotsConfig: OnPause=Full, OnCommit=Data",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnPause = SnapshotScopeFull
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeData
		},
		wantErr: false,
	}, {
		name: "SnapshotsConfig: OnPause=Data, OnCommit=Data",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnPause = SnapshotScopeData
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeData
		},
		wantErr: false,
	}, {
		name: "SnapshotsConfig: OnPause=Data, OnCommit=Full (invalid)",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnPause = SnapshotScopeData
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeFull
		},
		wantErr: true,
		errMsg:  "onCommit must be a subset of onPause",
	}, {
		name: "SnapshotsConfig: OnPause=Data, OnCommit unset (defaults to Full, invalid)",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnPause = SnapshotScopeData
		},
		wantErr: true,
		errMsg:  "onCommit must be a subset of onPause",
	}, {
		name: "SnapshotsConfig: OnPause unset (defaults to Full), OnCommit=Data",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeData
		},
		wantErr: false,
	}, {
		name: "SnapshotsConfig: OnPause invalid enum value",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnPause = SnapshotScope("bogus")
		},
		wantErr: true,
		errMsg:  "Unsupported value",
	}, {
		name: "SnapshotsConfig: OnCommit invalid enum value",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScope("bogus")
		},
		wantErr: true,
		errMsg:  "Unsupported value",
	}, {
		name: "SnapshotsConfig: onResume.fromData=Golden, microvm",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeData
			at.Spec.SnapshotsConfig.OnResume = OnResumeConfig{FromData: ResumeSourceGolden}
		},
		wantErr: false,
	}, {
		name: "SnapshotsConfig: onResume.fromData=ColdBoot, gvisor",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnResume = OnResumeConfig{FromData: ResumeSourceColdBoot}
		},
		wantErr: false,
	}, {
		name: "SnapshotsConfig: onResume.fromData invalid enum value",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
			at.Spec.SnapshotsConfig.OnResume = OnResumeConfig{FromData: ResumeSource("bogus")}
		},
		wantErr: true,
		errMsg:  "Unsupported value",
	}, {
		name: "SnapshotsConfig: onResume.fromData=Golden, explicit gvisor (invalid)",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassGvisor
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeData
			at.Spec.SnapshotsConfig.OnResume = OnResumeConfig{FromData: ResumeSourceGolden}
		},
		wantErr: true,
		errMsg:  "onResume.fromData: Golden is not supported when sandboxClass is 'gvisor'",
	}, {
		name: "SnapshotsConfig: onResume.fromData=Golden, SandboxClass unset (defaults to gvisor, invalid)",
		mutate: func(at *ActorTemplate) {
			at.Spec.SnapshotsConfig.OnCommit = SnapshotScopeData
			at.Spec.SnapshotsConfig.OnResume = OnResumeConfig{FromData: ResumeSourceGolden}
		},
		wantErr: true,
		errMsg:  "onResume.fromData: Golden is not supported when sandboxClass is 'gvisor'",
	}, {
		name: "Volumes: 1 DurableDir mount is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: 2 DurableDir volumes in template is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
				{Name: "vol2", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
				{Name: "vol2", MountPath: "/home2"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: 2 DurableDir volumes spread across containers is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
				{Name: "vol2", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers = append(at.Spec.Containers, Container{
				Name:  "sidecar",
				Image: "busybox@sha256:326e0e090a9a4057e62a1b94236e7a2df2f2f76722f67232e0e47854e4df9c53",
				VolumeMounts: []VolumeMount{
					{Name: "vol2", MountPath: "/home2"},
				},
			})
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: same DurableDir volume mounted twice in one container is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
				{Name: "vol1", MountPath: "/home2"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: same DurableDir volume mounted across two containers is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers = append(at.Spec.Containers, Container{
				Name:  "sidecar",
				Image: "busybox@sha256:326e0e090a9a4057e62a1b94236e7a2df2f2f76722f67232e0e47854e4df9c53",
				VolumeMounts: []VolumeMount{
					{Name: "vol1", MountPath: "/home-sidecar"},
				},
			})
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home-main"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: 1 ExternalVolumeTemplate mount is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{
					Name: "vol1",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("10Gi"),
							StorageClassName: "standard",
						},
					},
				},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/mnt/data"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: multiple ExternalVolumeTemplate mounts are valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{
					Name: "vol1",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("10Gi"),
							StorageClassName: "standard",
						},
					},
				},
				{
					Name: "vol2",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("20Gi"),
							StorageClassName: "pd-ssd",
						},
					},
				},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/mnt/vol1"},
				{Name: "vol2", MountPath: "/mnt/vol2"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: 1 DurableDir and 1 ExternalVolumeTemplate on same ActorTemplate is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "home", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
				{
					Name: "ext",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("10Gi"),
							StorageClassName: "standard",
						},
					},
				},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "home", MountPath: "/home"},
				{Name: "ext", MountPath: "/mnt/ext"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: VolumeSource with both DurableDir and ExternalVolumeTemplate set is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{
					Name: "vol1",
					VolumeSource: VolumeSource{
						DurableDir: &DurableDirVolumeSource{},
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("10Gi"),
							StorageClassName: "standard",
						},
					},
				},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/mnt/data"},
			}
		},
		wantErr: true,
		errMsg:  "exactly one of the fields in [durableDir externalVolumeTemplate] must be set",
	}, {
		name: "Volumes: VolumeSource with no source set is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{}},
			}
		},
		wantErr: true,
		errMsg:  "exactly one of the fields in [durableDir externalVolumeTemplate] must be set",
	}, {
		name: "Volumes: VolumeSource with no source set is invalid (mixed with a valid DurableDir volume)",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
				{Name: "vol2", VolumeSource: VolumeSource{}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
				{Name: "vol2", MountPath: "/mnt"},
			}
		},
		wantErr: true,
		errMsg:  "exactly one of the fields in [durableDir externalVolumeTemplate] must be set",
	}, {
		name: "Volumes: DurableDir MountPath with nested absolute path is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/user/data"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: DurableDir MountPath as bare root is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with relative path is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "home/user"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath as empty string is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: ""},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with leading whitespace is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: " /home"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with trailing slash is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with consecutive slashes is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home//user"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath containing ':' is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/ho:me"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with '..' component is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/../etc"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with trailing '..' is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/.."},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with '.' component is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/./user"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath containing dotfile is valid (only bare '.' / '..' components are rejected)",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/.config"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: DurableDir MountPath with segment starting with '..' is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/..config"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: DurableDir MountPath with embedded dots inside a segment is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/x..y"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: DurableDir MountPath with spaces is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/my home directory"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: DurableDir MountPath with NUL byte is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home\x00/user"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir MountPath with control character is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home\t/user"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: DurableDir mount with invalid MountPath in second container is rejected",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers = append(at.Spec.Containers, Container{
				Name:  "sidecar",
				Image: "busybox@sha256:326e0e090a9a4057e62a1b94236e7a2df2f2f76722f67232e0e47854e4df9c53",
				VolumeMounts: []VolumeMount{
					{Name: "vol1", MountPath: "home1"},
				},
			})
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
			}
		},
		wantErr: true,
		errMsg:  "MountPath must be a clean absolute Unix path",
	}, {
		name: "Volumes: Volume Name with uppercase is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "Vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
		},
		wantErr: true,
		errMsg:  "Name must be a valid DNS label",
	}, {
		name: "Volumes: Volume Name with underscore is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol_1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
		},
		wantErr: true,
		errMsg:  "Name must be a valid DNS label",
	}, {
		name: "Volumes: VolumeMount Name with uppercase is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "Vol1", MountPath: "/home/user"},
			}
		},
		wantErr: true,
		errMsg:  "Name must be a valid DNS label",
	}, {
		name: "Volumes: DurableDir volume with SandboxClass microvm is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/user"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: 2 DurableDir volumes with SandboxClass microvm is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
				{Name: "vol2", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
				{Name: "vol2", MountPath: "/home2"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: 2 DurableDir volumes spread across containers with SandboxClass microvm is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
				{Name: "vol2", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers = append(at.Spec.Containers, Container{
				Name:  "sidecar",
				Image: "busybox@sha256:326e0e090a9a4057e62a1b94236e7a2df2f2f76722f67232e0e47854e4df9c53",
				VolumeMounts: []VolumeMount{
					{Name: "vol2", MountPath: "/home2"},
				},
			})
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: same DurableDir volume mounted twice in one container with SandboxClass microvm is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home1"},
				{Name: "vol1", MountPath: "/home2"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: DurableDir volume with SandboxClass gvisor is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassGvisor
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/home/user"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: ExternalVolumeTemplate volume with SandboxClass microvm is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
			at.Spec.Volumes = []Volume{
				{
					Name: "vol1",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("10Gi"),
							StorageClassName: "standard",
						},
					},
				},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/mnt/data"},
			}
		},
		wantErr: true,
		errMsg:  "ExternalVolumes are not supported when sandboxClass is 'microvm'",
	}, {
		name: "Volumes: ExternalVolumeTemplate volume with SandboxClass gvisor is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassGvisor
			at.Spec.Volumes = []Volume{
				{
					Name: "vol1",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("10Gi"),
							StorageClassName: "standard",
						},
					},
				},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/mnt/data"},
			}
		},
		wantErr: false,
	}, {
		name: "Volumes: SandboxClass microvm without DurableDir volumes is valid",
		mutate: func(at *ActorTemplate) {
			at.Spec.SandboxClass = SandboxClassMicroVM
		},
		wantErr: false,
	}, {
		name: "Volumes: volume without volumeMount is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{Name: "vol1", VolumeSource: VolumeSource{DurableDir: &DurableDirVolumeSource{}}},
			}
		},
		wantErr: true,
		errMsg:  "All volumes defined in spec.volumes must be mounted by at least one container",
	}, {
		name: "Volumes: multiple volumes with one unmounted is invalid",
		mutate: func(at *ActorTemplate) {
			at.Spec.Volumes = []Volume{
				{
					Name: "vol1",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("10Gi"),
							StorageClassName: "standard",
						},
					},
				},
				{
					Name: "vol2",
					VolumeSource: VolumeSource{
						ExternalVolumeTemplate: &ExternalVolumeTemplate{
							Capacity:         resource.MustParse("20Gi"),
							StorageClassName: "pd-ssd",
						},
					},
				},
			}
			at.Spec.Containers[0].VolumeMounts = []VolumeMount{
				{Name: "vol1", MountPath: "/mnt/vol1"},
			}
		},
		wantErr: true,
		errMsg:  "All volumes defined in spec.volumes must be mounted by at least one container",
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
				// Create writes the API server's response back into at, so
				// verify sees the object as stored — defaults included.
				if tt.verify != nil {
					tt.verify(t, at)
				}
				_ = k8sClient.Delete(ctx, at)
			}
		})
	}
}

func TestActorTemplateSpecImmutability(t *testing.T) {
	ctx := t.Context()

	baseTemplate := &ActorTemplate{
		Spec: ActorTemplateSpec{
			Containers: []Container{
				{
					Name:  "main",
					Image: "busybox@hash",
				},
			},
			SnapshotsConfig: SnapshotsConfig{
				Location: "gs://test-bucket/test-folder",
			},
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"pool": "test-pool"},
			},
		},
	}

	tests := []struct {
		name   string
		mutate func(*ActorTemplate)
	}{
		{
			name: "update-container-image",
			mutate: func(at *ActorTemplate) {
				at.Spec.Containers[0].Image = "busybox@new"
			},
		},
		{
			name: "update-snapshots-config-location",
			mutate: func(at *ActorTemplate) {
				at.Spec.SnapshotsConfig.Location = "gs://new-bucket/new-folder"
			},
		},
		{
			name: "update-worker-selector",
			mutate: func(at *ActorTemplate) {
				at.Spec.WorkerSelector.MatchLabels["pool"] = "new-pool"
			},
		},
		{
			name: "update-sandbox-class",
			mutate: func(at *ActorTemplate) {
				at.Spec.SandboxClass = SandboxClassMicroVM
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
