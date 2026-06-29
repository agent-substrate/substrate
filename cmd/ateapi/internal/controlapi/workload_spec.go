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
	"fmt"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const envSecretCacheTTL = 30 * time.Second

// workloadSpecFromActorTemplate builds a WorkloadSpec without resolving
// container env vars. Use this when downstream consumers (e.g. checkpoint
// requests) don't need env entries materialized.
func workloadSpecFromActorTemplate(actorTemplate *atev1alpha1.ActorTemplate) *ateletpb.WorkloadSpec {
	workloadSpec := &ateletpb.WorkloadSpec{
		PauseImage: actorTemplate.Spec.PauseImage,
	}

	// add volumes
	for _, vol := range actorTemplate.Spec.Volumes {
		// volume is durable-dir type
		if vol.VolumeSource.DurableDir != nil {
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.Name,
				Type: ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR,
				Source: &ateletpb.Volume_DurableDir{
					DurableDir: &ateletpb.DurableDirVolume{},
				},
			})
		}
	}

	for _, ctr := range actorTemplate.Spec.Containers {
		ateletCtr := &ateletpb.Container{
			Name:    ctr.Name,
			Image:   ctr.Image,
			Command: ctr.Command,
			Readyz:  toAteletReadyz(ctr.Readyz),
		}
		for _, mount := range ctr.VolumeMounts {
			ateletCtr.VolumeMounts = append(ateletCtr.VolumeMounts, &ateletpb.VolumeMount{
				Name:      mount.Name,
				MountPath: mount.MountPath,
			})
		}
		workloadSpec.Containers = append(workloadSpec.Containers, ateletCtr)
	}

	return workloadSpec
}

// workloadSpecFromActorTemplateWithEnv builds a WorkloadSpec and resolves each
// container's env vars against the cluster. kubeClient must be non-nil;
// secretCache is optional and, when supplied, deduplicates Secret reads.
func workloadSpecFromActorTemplateWithEnv(ctx context.Context, kubeClient kubernetes.Interface, secretCache *envSecretCache, actorTemplate *atev1alpha1.ActorTemplate) (*ateletpb.WorkloadSpec, error) {
	workloadSpec := workloadSpecFromActorTemplate(actorTemplate)

	resolver := envResolver{
		kubeClient: kubeClient,
		namespace:  actorTemplate.Namespace,
		cache:      secretCache,
	}

	for i, ctr := range actorTemplate.Spec.Containers {
		for _, env := range ctr.Env {
			ateletEnv, err := resolver.resolve(ctx, ctr.Name, env)
			if err != nil {
				return nil, err
			}
			if ateletEnv != nil {
				workloadSpec.Containers[i].Env = append(workloadSpec.Containers[i].Env, ateletEnv)
			}
		}
	}

	return workloadSpec, nil
}

// toAteletReadyz projects the CRD readyz field onto the ateletpb wire type.
// Returns nil when the source is nil so containers without a probe stay
// unchanged on the wire.
func toAteletReadyz(in *atev1alpha1.ContainerReadyz) *ateletpb.Readyz {
	if in == nil {
		return nil
	}
	out := &ateletpb.Readyz{}
	if in.HTTPGet != nil {
		out.HttpGet = &ateletpb.HTTPGetAction{
			Path: in.HTTPGet.Path,
			Port: in.HTTPGet.Port,
		}
	}
	return out
}

type envResolver struct {
	kubeClient kubernetes.Interface
	namespace  string
	cache      *envSecretCache
}

func (r *envResolver) resolve(ctx context.Context, containerName string, env atev1alpha1.EnvVar) (*ateletpb.EnvEntry, error) {
	envID := fmt.Sprintf("container %q env %q", containerName, env.Name)

	switch {
	case env.Value != nil:
		return &ateletpb.EnvEntry{
			Name:  env.Name,
			Value: *env.Value,
		}, nil
	case env.ValueFrom != nil:
		value, include, err := r.resolveValueFrom(ctx, envID, env.ValueFrom)
		if err != nil {
			return nil, err
		}
		if !include {
			return nil, nil
		}
		return &ateletpb.EnvEntry{
			Name:  env.Name,
			Value: value,
		}, nil
	}
	return nil, status.Errorf(codes.FailedPrecondition, "%s has unknown value source", envID)
}

func (r *envResolver) resolveValueFrom(ctx context.Context, envID string, valueFrom *atev1alpha1.EnvVarSource) (string, bool, error) {
	if ref := valueFrom.SecretKeyRef; ref != nil {
		return r.resolveSecretKeyRef(ctx, envID, ref)
	}
	return "", false, status.Errorf(codes.FailedPrecondition, "%s uses unsupported valueFrom source; only secretKeyRef is supported", envID)
}

func (r *envResolver) resolveSecretKeyRef(ctx context.Context, envID string, ref *atev1alpha1.SecretKeySelector) (string, bool, error) {
	if r.kubeClient == nil {
		return "", false, status.Errorf(codes.FailedPrecondition, "%s cannot resolve secretKeyRef because Kubernetes client is unavailable", envID)
	}

	secret, err := r.secret(ctx, ref.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if isOptional(ref.Optional) {
				return "", false, nil
			}
			return "", false, status.Errorf(codes.FailedPrecondition, "%s references missing secret %s/%s", envID, r.namespace, ref.Name)
		}
		return "", false, status.Errorf(codes.Internal, "while resolving %s secretKeyRef %s/%s: %v", envID, r.namespace, ref.Name, err)
	}

	value, ok := secret.Data[ref.Key]
	if !ok {
		if isOptional(ref.Optional) {
			return "", false, nil
		}
		return "", false, status.Errorf(codes.FailedPrecondition, "%s references missing key %q in secret %s/%s", envID, ref.Key, r.namespace, ref.Name)
	}

	return string(value), true, nil
}

func (r *envResolver) secret(ctx context.Context, name string) (*corev1.Secret, error) {
	if r.cache != nil {
		return r.cache.get(ctx, r.kubeClient, r.namespace, name)
	}
	return r.kubeClient.CoreV1().Secrets(r.namespace).Get(ctx, name, metav1.GetOptions{})
}

type envSecretCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[envSecretCacheKey]envSecretCacheEntry
}

type envSecretCacheKey struct {
	namespace string
	name      string
}

type envSecretCacheEntry struct {
	secret    *corev1.Secret
	expiresAt time.Time
}

func newEnvSecretCache(ttl time.Duration) *envSecretCache {
	return &envSecretCache{
		ttl:     ttl,
		entries: map[envSecretCacheKey]envSecretCacheEntry{},
	}
}

func (c *envSecretCache) get(ctx context.Context, kubeClient kubernetes.Interface, namespace, name string) (*corev1.Secret, error) {
	key := envSecretCacheKey{
		namespace: namespace,
		name:      name,
	}
	now := time.Now()

	c.mu.RLock()
	entry, ok := c.entries[key]
	if ok && now.Before(entry.expiresAt) {
		secret := entry.secret.DeepCopy()
		c.mu.RUnlock()
		return secret, nil
	}
	c.mu.RUnlock()

	// TODO: Make refresh smarter if this pattern sticks, for example by
	// refreshing asynchronously or watching referenced Secrets.
	secret, err := kubeClient.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	secret = secret.DeepCopy()
	c.mu.Lock()
	c.entries[key] = envSecretCacheEntry{
		secret:    secret,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()

	return secret.DeepCopy(), nil
}

func isOptional(optional *bool) bool {
	return optional != nil && *optional
}
