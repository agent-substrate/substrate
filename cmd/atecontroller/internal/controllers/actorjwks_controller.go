// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/oidcdiscovery"
)

// ActorJWKSReconciler publishes the public keys of the actor JWT pool as a JWK
// set in a ConfigMap, which ate-idp-server serves.
type ActorJWKSReconciler struct {
	client.Client

	// SystemNamespace is the namespace holding the pool Secret and the
	// ConfigMap.
	SystemNamespace string
}

// ActorJWTPoolRef names the Secret holding the authorities ateapi signs actor
// JWTs with.
func ActorJWTPoolRef(systemNamespace string) types.NamespacedName {
	return types.NamespacedName{Namespace: systemNamespace, Name: "actor-id-jwt-pool"}
}

// ActorJWKSRef names the ConfigMap holding the JWK set. Its name, keys, and
// format are internal to the install, not a supported way to read keys.
func ActorJWKSRef(systemNamespace string) types.NamespacedName {
	return types.NamespacedName{Namespace: systemNamespace, Name: "actor-id-jwks"}
}

// actorJWKSKey is the ConfigMap key holding the JWK set.
const actorJWKSKey = "jwks.json"

//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch,namespace=ate-system

func (r *ActorJWKSReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Once the pool is gone or going, garbage collection removes the ConfigMap
	// it owns.
	secret := &corev1.Secret{}
	if err := r.Get(ctx, req.NamespacedName, secret); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get actor JWT pool %q: %w", req.NamespacedName, err)
	}
	if !secret.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, nil
	}

	jwks, err := actorJWKS(secret)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to derive the actor JWK set from %q: %w", req.NamespacedName, err)
	}

	ref := ActorJWKSRef(r.SystemNamespace)
	cm := corev1ac.ConfigMap(ref.Name, ref.Namespace).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion("v1").
			WithKind("Secret").
			WithName(secret.Name).
			WithUID(secret.UID).
			WithController(true)).
		WithData(map[string]string{actorJWKSKey: string(jwks)})

	const actorJWKSFieldOwner = "ate-actor-jwks"
	if err := r.Apply(ctx, cm, client.FieldOwner(actorJWKSFieldOwner), client.ForceOwnership); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to apply ConfigMap %q: %w", ref, err)
	}
	log.Info("reconciled the actor JWK set",
		"configmap", ref.String(),
		"secret", req.NamespacedName.String())

	return ctrl.Result{}, nil
}

// actorJWKS renders the public key of every authority in the pool, active or
// not, as a JWK set.
func actorJWKS(secret *corev1.Secret) ([]byte, error) {
	// The key `kubectl-ate admin make-jwt-pool` writes the marshaled pool under.
	const actorJWTPoolKey = "pool"

	wire, ok := secret.Data[actorJWTPoolKey]
	if !ok {
		return nil, fmt.Errorf("secret has no %q key", actorJWTPoolKey)
	}
	pool, err := localjwtauthority.Unmarshal(wire)
	if err != nil {
		return nil, fmt.Errorf("parsing the JWT pool: %w", err)
	}
	verificationKeys, err := pool.VerificationKeys()
	if err != nil {
		return nil, fmt.Errorf("reading the JWT pool's verification keys: %w", err)
	}

	keys := make([]oidcdiscovery.PublicKey, 0, len(verificationKeys))
	for _, vk := range verificationKeys {
		keys = append(keys, oidcdiscovery.PublicKey{ID: vk.KeyID, Algorithm: vk.Algorithm, Key: vk.PublicKey})
	}
	return oidcdiscovery.JWKS(keys)
}

func (r *ActorJWKSReconciler) SetupWithManager(mgr ctrl.Manager) error {
	poolRef := ActorJWTPoolRef(r.SystemNamespace)
	jwksRef := ActorJWKSRef(r.SystemNamespace)

	// The ConfigMap is watched so that deleting or hand-editing it is
	// reverted.
	return ctrl.NewControllerManagedBy(mgr).
		Named("actorjwks").
		For(&corev1.Secret{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			return obj.GetNamespace() == poolRef.Namespace && obj.GetName() == poolRef.Name
		}))).
		Watches(&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: poolRef}}
			}),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetNamespace() == jwksRef.Namespace && obj.GetName() == jwksRef.Name
			}))).
		Complete(r)
}
