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
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestWorkloadSpecFromActorTemplateResolvesValueFromEnv(t *testing.T) {
	ctx := context.Background()
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

	got, err := workloadSpecFromActorTemplate(ctx, kubeClient, nil, &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tmpl1",
			Namespace: "agent-ns",
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			PauseImage: "pause",
			Containers: []atev1alpha1.Container{
				{
					Name:    "main",
					Image:   "main",
					Command: []string{"/main"},
					Env: []atev1alpha1.EnvVar{
						{
							Name:  "LITERAL",
							Value: ptr.To("plain"),
						},
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
	})
	if err != nil {
		t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
	}

	want := &ateletpb.WorkloadSpec{
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
						{
							Name: "OPTIONAL",
							ValueFrom: &atev1alpha1.EnvVarSource{
								SecretKeyRef: &atev1alpha1.SecretKeySelector{
									Name:     "missing",
									Key:      "key",
									Optional: &optional,
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
	}
	if len(got.GetContainers()) != 1 {
		t.Fatalf("expected one container, got %d", len(got.GetContainers()))
	}
	if len(got.GetContainers()[0].GetEnv()) != 0 {
		t.Fatalf("expected optional missing env to be skipped, got %v", got.GetContainers()[0].GetEnv())
	}
}

func TestWorkloadSpecFromActorTemplateSecretKeyRefMissingSecretFails(t *testing.T) {
	_, err := workloadSpecFromActorTemplate(context.Background(), fake.NewSimpleClientset(), nil, &atev1alpha1.ActorTemplate{
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
							Name: "REQUIRED",
							ValueFrom: &atev1alpha1.EnvVarSource{
								SecretKeyRef: &atev1alpha1.SecretKeySelector{
									Name: "missing",
									Key:  "key",
								},
							},
						},
					},
				},
			},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v: %v", status.Code(err), err)
	}
}

func TestWorkloadSpecFromActorTemplateEmptyValueFromFails(t *testing.T) {
	_, err := workloadSpecFromActorTemplate(context.Background(), fake.NewSimpleClientset(), nil, &atev1alpha1.ActorTemplate{
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
							Name:      "EMPTY",
							ValueFrom: &atev1alpha1.EnvVarSource{},
						},
					},
				},
			},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v: %v", status.Code(err), err)
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
