//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package controlapi

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestWorkloadSpecFromActorTemplate(t *testing.T) {
	tests := []struct {
		name        string
		secrets     []runtime.Object
		template    *atev1alpha1.ActorTemplate
		want        *ateletpb.WorkloadSpec
		wantErrCode codes.Code
	}{
		{
			name: "resolves literal and secretKeyRef env",
			secrets: []runtime.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: "some-secret", Namespace: "agent-ns"},
					Data:       map[string][]byte{"some-key": []byte("some-value")},
				},
			},
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					PauseImage: "pause",
					Containers: []atev1alpha1.Container{
						{
							Name:    "main",
							Image:   "main",
							Command: []string{"/main"},
							Env: []atev1alpha1.EnvVar{
								{Name: "LITERAL", Value: ptr.To("plain")},
								{
									Name: "SOME_KEY",
									ValueFrom: &atev1alpha1.EnvVarSource{
										SecretKeyRef: &atev1alpha1.SecretKeySelector{Name: "some-secret", Key: "some-key"},
									},
								},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				PauseImage: "pause",
				Containers: []*ateletpb.Container{
					{
						Name:    "main",
						Image:   "main",
						Command: []string{"/main"},
						Env: []*ateletpb.EnvEntry{
							{Name: "LITERAL", Value: "plain"},
							{Name: "SOME_KEY", Value: "some-value"},
						},
					},
				},
			},
		},
<<<<<<< HEAD
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("WorkloadSpec mismatch (-want +got):\n%s", diff)
	}
}

func TestWorkloadSpecFromActorTemplatePropagatesReadyz(t *testing.T) {
	ctx := context.Background()
	got, err := workloadSpecFromActorTemplate(ctx, fake.NewSimpleClientset(), nil, &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-readyz", Namespace: "agent-ns"},
		Spec: atev1alpha1.ActorTemplateSpec{
			Containers: []atev1alpha1.Container{
				{
					Name:  "with-probe",
					Image: "main",
					Readyz: &atev1alpha1.ContainerReadyz{
						HTTPGet: &atev1alpha1.HTTPGetAction{Path: "/health", Port: 8080},
					},
				},
				{
					Name:  "without-probe",
					Image: "side",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
	}

	want := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{
			{
				Name:  "with-probe",
				Image: "main",
				Readyz: &ateletpb.Readyz{
					HttpGet: &ateletpb.HTTPGetAction{Path: "/health", Port: 8080},
				},
			},
			{
				Name:  "without-probe",
				Image: "side",
			},
		},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("WorkloadSpec mismatch (-want +got):\n%s", diff)
	}
}

func TestWorkloadSpecFromActorTemplateOptionalSecretKeyRefSkipsMissingSecret(t *testing.T) {
	optional := true
	got, err := workloadSpecFromActorTemplate(context.Background(), fake.NewSimpleClientset(), nil, &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tmpl1",
			Namespace: "agent-ns",
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			Containers: []atev1alpha1.Container{
				{
					Name:  "main",
					Image: "main",
					Env: []atev1alpha1.EnvVar{
=======
		{
			name: "skips optional missing secret",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Containers: []atev1alpha1.Container{
>>>>>>> 3962a2c (implement suspend/resume with homedir support)
						{
							Name:  "main",
							Image: "main",
							Env: []atev1alpha1.EnvVar{
								{
									Name: "OPTIONAL",
									ValueFrom: &atev1alpha1.EnvVarSource{
										SecretKeyRef: &atev1alpha1.SecretKeySelector{Name: "missing", Key: "key", Optional: ptr.To(true)},
									},
								},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{{Name: "main", Image: "main"}},
			},
		},
		{
			name: "required missing secret fails",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							Env: []atev1alpha1.EnvVar{
								{
									Name: "REQUIRED",
									ValueFrom: &atev1alpha1.EnvVarSource{
										SecretKeyRef: &atev1alpha1.SecretKeySelector{Name: "missing", Key: "key"},
									},
								},
							},
						},
					},
				},
			},
			wantErrCode: codes.FailedPrecondition,
		},
		{
			name: "empty valueFrom fails",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							Env: []atev1alpha1.EnvVar{
								{Name: "EMPTY", ValueFrom: &atev1alpha1.EnvVarSource{}},
							},
						},
					},
				},
			},
			wantErrCode: codes.FailedPrecondition,
		},
		{
			name: "converts HomeDir volume and mounts",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					PauseImage: "pause",
					Volumes: []atev1alpha1.Volume{
						{Name: "home", VolumeSource: atev1alpha1.VolumeSource{HomeDir: &atev1alpha1.HomedirVolumeSource{}}},
					},
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							VolumeMounts: []atev1alpha1.VolumeMount{
								{Name: "home", MountPath: "/home/user"},
								{Name: "home", MountPath: "/workspace"},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				PauseImage: "pause",
				Volumes: []*ateletpb.Volume{
					{
						Name:   "home",
						Type:   ateletpb.VolumeType_VOLUME_TYPE_HOMEDIR,
						Source: &ateletpb.Volume_HomeDir{HomeDir: &ateletpb.HomedirVolume{}},
					},
				},
				Containers: []*ateletpb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "home", MountPath: "/home/user"},
							{Name: "home", MountPath: "/workspace"},
						},
					},
				},
			},
		},
		{
			name: "skips non-HomeDir volumes",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{Name: "unsupported", VolumeSource: atev1alpha1.VolumeSource{}},
						{Name: "home", VolumeSource: atev1alpha1.VolumeSource{HomeDir: &atev1alpha1.HomedirVolumeSource{}}},
					},
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							VolumeMounts: []atev1alpha1.VolumeMount{
								{Name: "home", MountPath: "/workspace"},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:   "home",
						Type:   ateletpb.VolumeType_VOLUME_TYPE_HOMEDIR,
						Source: &ateletpb.Volume_HomeDir{HomeDir: &ateletpb.HomedirVolume{}},
					},
				},
				Containers: []*ateletpb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "home", MountPath: "/workspace"},
						},
					},
				},
			},
		},
		{
			name: "container without volume mounts has none",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{Name: "home", VolumeSource: atev1alpha1.VolumeSource{HomeDir: &atev1alpha1.HomedirVolumeSource{}}},
					},
					Containers: []atev1alpha1.Container{
						{Name: "main", Image: "main"},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:   "home",
						Type:   ateletpb.VolumeType_VOLUME_TYPE_HOMEDIR,
						Source: &ateletpb.Volume_HomeDir{HomeDir: &ateletpb.HomedirVolume{}},
					},
				},
				Containers: []*ateletpb.Container{{Name: "main", Image: "main"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kubeClient := fake.NewSimpleClientset(tt.secrets...)
			got, err := workloadSpecFromActorTemplate(context.Background(), kubeClient, nil, tt.template)
			if tt.wantErrCode != codes.OK {
				if status.Code(err) != tt.wantErrCode {
					t.Fatalf("error code = %v, want %v: %v", status.Code(err), tt.wantErrCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("WorkloadSpec mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWorkloadSpecFromActorTemplateCachesSecretsAcrossCalls(t *testing.T) {
	ctx := context.Background()
	secretCache := newEnvSecretCache(envSecretCacheTTL)
	kubeClient := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "some-secret",
				Namespace: "agent-ns",
			},
			Data: map[string][]byte{
				"some-key": []byte("some-value"),
			},
		},
	)
	actorTemplate := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tmpl1",
			Namespace: "agent-ns",
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			Containers: []atev1alpha1.Container{
				{
					Name:  "main",
					Image: "main",
					Env: []atev1alpha1.EnvVar{
						{
							Name: "SOME_KEY",
							ValueFrom: &atev1alpha1.EnvVarSource{
								SecretKeyRef: &atev1alpha1.SecretKeySelector{
									Name: "some-secret",
									Key:  "some-key",
								},
							},
						},
					},
				},
			},
		},
	}

	if _, err := workloadSpecFromActorTemplate(ctx, kubeClient, secretCache, actorTemplate); err != nil {
		t.Fatalf("first workloadSpecFromActorTemplate failed: %v", err)
	}
	if _, err := workloadSpecFromActorTemplate(ctx, kubeClient, secretCache, actorTemplate); err != nil {
		t.Fatalf("second workloadSpecFromActorTemplate failed: %v", err)
	}
	if got := secretGetCount(kubeClient); got != 1 {
		t.Fatalf("secret gets before TTL expiry = %d, want 1", got)
	}

	expireSecretCache(secretCache)
	if _, err := workloadSpecFromActorTemplate(ctx, kubeClient, secretCache, actorTemplate); err != nil {
		t.Fatalf("third workloadSpecFromActorTemplate failed: %v", err)
	}
	if got := secretGetCount(kubeClient); got != 2 {
		t.Fatalf("secret gets after TTL expiry = %d, want 2", got)
	}
}

func expireSecretCache(secretCache *envSecretCache) {
	secretCache.mu.Lock()
	defer secretCache.mu.Unlock()

	for key, entry := range secretCache.entries {
		entry.expiresAt = entry.expiresAt.Add(-envSecretCacheTTL)
		secretCache.entries[key] = entry
	}
}

func secretGetCount(kubeClient *fake.Clientset) int {
	count := 0
	for _, action := range kubeClient.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "secrets" {
			count++
		}
	}
	return count
}
